package com.ainovel.server.web.controller;

import com.ainovel.server.common.response.ApiResponse;
import com.ainovel.server.domain.model.EnhancedUserPromptTemplate;
import com.ainovel.server.domain.model.ReviewStatusConstants;
import com.ainovel.server.domain.model.Novel;
import com.ainovel.server.domain.model.setting.generation.SettingGenerationEvent;
import com.ainovel.server.domain.model.setting.generation.SettingGenerationSession;
import com.ainovel.server.domain.model.settinggeneration.NodeTemplateConfig;
import com.ainovel.server.service.setting.generation.ISettingGenerationService;
import com.ainovel.server.service.setting.generation.StrategyManagementService;
import com.ainovel.server.service.setting.NovelSettingHistoryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
// import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 设定生成控制器
 * 提供AI驱动的结构化小说设定生成API
 * 
 * 设定生成与历史记录关系说明：
 * 1. 设定历史记录与小说无关，与用户有关 - 历史记录是按用户维度管理的
 * 2. 小说与历史记录的关系：
 *    a) 当用户进入小说设定生成页面时，如果没有历史记录，会创建一个历史记录，收集当前小说的设定作为快照
 *    b) 用户从小说列表页面发起提示词生成设定请求，生成完后会自动生成一个历史记录
 * 3. 历史记录相当于小说设定的快照，供用户修改和版本管理
 * 4. 设定生成流程：
 *    - 用户输入提示词 -> AI生成设定结构 -> 用户可修改节点 -> 保存到小说设定 -> 自动创建历史记录
 * 5. 编辑现有设定流程：
 *    - 从历史记录创建编辑会话 -> 修改设定节点 -> 保存修改 -> 更新历史记录或创建新历史记录
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/setting-generation")
@RequiredArgsConstructor
@Tag(name = "设定生成", description = "AI驱动的结构化小说设定生成")
public class SettingGenerationController {
    
    private final ISettingGenerationService settingGenerationService;
    @SuppressWarnings("unused") // 保留用于未来功能
    private final NovelSettingHistoryService historyService;
    private final StrategyManagementService strategyManagementService;
    private final com.ainovel.server.service.setting.generation.SystemStrategyInitializationService systemStrategyInitializationService;
    private final com.ainovel.server.repository.EnhancedUserPromptTemplateRepository templateRepository;
    private final com.ainovel.server.service.NovelService novelService;
    private final com.ainovel.server.service.setting.generation.InMemorySessionManager sessionManager;
    private final com.ainovel.server.service.setting.SettingComposeService settingComposeService;
    
    /**
     * 获取可用的生成策略模板
     */
    @GetMapping("/strategies")
    @Operation(summary = "获取可用的生成策略模板", description = "返回所有支持的设定生成策略模板列表")
    public Mono<ApiResponse<List<ISettingGenerationService.StrategyTemplateInfo>>> getAvailableStrategyTemplates(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser) {
        Mono<List<ISettingGenerationService.StrategyTemplateInfo>> mono =
            (currentUser != null && currentUser.getId() != null)
                ? ((com.ainovel.server.service.setting.generation.SettingGenerationService)settingGenerationService).getAvailableStrategyTemplatesForUser(currentUser.getId())
                : settingGenerationService.getAvailableStrategyTemplates();
        return mono.map(ApiResponse::success)
            .onErrorResume(error -> {
                log.error("Failed to get available strategy templates", error);
                return Mono.just(ApiResponse.error("GET_STRATEGIES_FAILED", error.getMessage()));
            });
    }
    
    /**
     * 启动设定生成
     * 用户从小说列表页面发起提示词生成设定请求时调用
     */
    @PostMapping(value = "/start", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "启动设定生成", 
        description = "根据用户提示词和选定策略开始生成设定，返回SSE事件流。生成完成后会自动创建历史记录")
    public Flux<ServerSentEvent<SettingGenerationEvent>> startGeneration(
            @Valid @RequestBody StartGenerationRequest request) {
        
        // 📚 自定义验证：检查请求是否有效（复用模式可以无提示词）
        if (!request.isValid()) {
            boolean isReuseMode = "REUSE".equalsIgnoreCase(request.getKnowledgeBaseMode());
            String errorMsg = isReuseMode 
                ? "复用模式需要提供策略" 
                : "非复用模式需要提供初始提示词和策略";
            
            return Flux.just(ServerSentEvent.<SettingGenerationEvent>builder()
                .event("GenerationErrorEvent")
                .data(new SettingGenerationEvent.GenerationErrorEvent() {{
                    setErrorCode("INVALID_REQUEST");
                    setErrorMessage(errorMsg);
                    setRecoverable(false);
                }})
                .build());
        }
        
        // 使用请求中的userId，如果没有提供则使用默认值
        String userId = request.getUserId() != null ? request.getUserId() : "67d67d6833335f5166782e6f";
        
        // 🔧 结构化输出循环模式路由
        if (Boolean.TRUE.equals(request.getUseStructuredOutput())) {
            Integer iterations = request.getStructuredIterations() != null ? request.getStructuredIterations() : 3;
            log.info("[StructuredOutput] 使用结构化输出循环模式，最大迭代次数: {}", iterations);
            
            // 处理promptTemplateId
            Mono<String> promptTemplateIdMono;
            if (request.getPromptTemplateId() != null && !request.getPromptTemplateId().trim().isEmpty()) {
                promptTemplateIdMono = Mono.just(request.getPromptTemplateId());
            } else if (request.getStrategy() != null && !request.getStrategy().trim().isEmpty()) {
                promptTemplateIdMono = strategyManagementService.findTemplateIdByStrategyName(request.getStrategy());
            } else {
                return Flux.just(ServerSentEvent.<SettingGenerationEvent>builder()
                    .event("GenerationErrorEvent")
                    .data(new SettingGenerationEvent.GenerationErrorEvent() {{
                        setErrorCode("INVALID_REQUEST");
                        setErrorMessage("必须提供promptTemplateId或strategy");
                        setRecoverable(false);
                    }})
                    .build());
            }
            
            return promptTemplateIdMono
                .flatMapMany(actualPromptTemplateId -> {
                    // 📚 直接传递原始的知识库分组参数，不合并
                    // 🔧 传递前端生成的sessionId（如果有的话）
                    return settingGenerationService.startGenerationStructuredWithKnowledgeBase(
                        request.getSessionId(),  // 前端生成的sessionId
                        userId,
                        request.getNovelId(),
                        request.getInitialPrompt(),
                        actualPromptTemplateId,
                        request.getModelConfigId(),
                        iterations,
                        request.getKnowledgeBaseMode(),
                        request.getKnowledgeBaseIds(),  // REUSE/IMITATION模式使用
                        request.getReuseKnowledgeBaseIds(),  // HYBRID模式：用于复用
                        request.getReferenceKnowledgeBaseIds(),  // HYBRID模式：用于参考
                        request.getKnowledgeBaseCategories()
                    )
                    .flatMapMany(session -> 
                        settingGenerationService.getGenerationEventStream(session.getSessionId())
                    )
                    .map(event -> ServerSentEvent.<SettingGenerationEvent>builder()
                        .event(event.getClass().getSimpleName())
                        .data(event)
                        .build()
                    )
                    .onErrorResume(error -> {
                        log.error("[StructuredOutput] 生成失败: {}", error.getMessage(), error);
                        return Flux.just(ServerSentEvent.<SettingGenerationEvent>builder()
                            .event("GenerationErrorEvent")
                            .data(new SettingGenerationEvent.GenerationErrorEvent() {{
                                setErrorCode("GENERATION_FAILED");
                                setErrorMessage("结构化输出生成失败: " + error.getMessage());
                                setRecoverable(false);
                            }})
                            .build());
                    });
                });
        }
        
        // 兼容性处理：如果提供了strategy而没有promptTemplateId，则转换
        Mono<String> promptTemplateIdMono;
        if (request.getPromptTemplateId() != null && !request.getPromptTemplateId().trim().isEmpty()) {
            promptTemplateIdMono = Mono.just(request.getPromptTemplateId());
        } else if (request.getStrategy() != null && !request.getStrategy().trim().isEmpty()) {
            log.warn("使用已废弃的strategy参数: {}, 建议使用promptTemplateId", request.getStrategy());
            // 通过SystemStrategyInitializationService查找对应的模板ID
            promptTemplateIdMono = systemStrategyInitializationService.getTemplateIdByStrategyId(request.getStrategy())
                .doOnNext(templateId -> log.info("策略 {} 转换为模板ID: {}", request.getStrategy(), templateId));
        } else {
            return Flux.just(ServerSentEvent.<SettingGenerationEvent>builder()
                .event("GenerationErrorEvent")
                .data(new SettingGenerationEvent.GenerationErrorEvent() {{
                    setErrorCode("INVALID_REQUEST");
                    setErrorMessage("必须提供promptTemplateId或strategy参数");
                    setRecoverable(false);
                }})
                .build());
        }
        
        // 创建会话并获取事件流（切换到"新流程：Hybrid"）
              return promptTemplateIdMono.<ServerSentEvent<SettingGenerationEvent>>flatMapMany(promptTemplateId -> {
            log.info("[新流程][HYBRID] 启动设定生成: 用户={}, 模板ID={}, 模型配置ID={}, 小说ID={}, 知识库模式={}",
                userId, promptTemplateId, request.getModelConfigId(), request.getNovelId(), 
                request.getKnowledgeBaseMode());

            // 📚 根据是否有知识库参数决定调用哪个方法
            Mono<SettingGenerationSession> sessionMono;
            
            if (request.getKnowledgeBaseMode() != null && 
                !"NONE".equalsIgnoreCase(request.getKnowledgeBaseMode())) {
                
                String mode = request.getKnowledgeBaseMode();
                
                // 📚 混合模式：使用独立的复用和参考参数
                if ("HYBRID".equalsIgnoreCase(mode) && 
                    request.getReuseKnowledgeBaseIds() != null && 
                    !request.getReuseKnowledgeBaseIds().isEmpty()) {
                    
                    log.info("[KB-Integration] 使用知识库混合流程: reuse={}, reference={}", 
                            request.getReuseKnowledgeBaseIds(), request.getReferenceKnowledgeBaseIds());
                    
                    sessionMono = settingGenerationService.startGenerationWithKnowledgeBaseHybrid(
                            userId,
                            request.getNovelId(),
                            request.getInitialPrompt(),
                            promptTemplateId,
                            request.getModelConfigId(),
                            request.getUsePublicTextModel(),
                            request.getReuseKnowledgeBaseIds(),
                            request.getReferenceKnowledgeBaseIds(),
                            request.getKnowledgeBaseCategories()
                    );
                }
                // 📚 复用/仿写模式：使用通用的knowledgeBaseIds
                else if (request.getKnowledgeBaseIds() != null && !request.getKnowledgeBaseIds().isEmpty()) {
                    log.info("[KB-Integration] 使用知识库集成流程: mode={}, KBs={}", 
                            request.getKnowledgeBaseMode(), request.getKnowledgeBaseIds());
                    
                    sessionMono = settingGenerationService.startGenerationWithKnowledgeBase(
                            userId,
                            request.getNovelId(),
                            request.getInitialPrompt(),
                            promptTemplateId,
                            request.getModelConfigId(),
                            request.getUsePublicTextModel(),
                            request.getKnowledgeBaseMode(),
                            request.getKnowledgeBaseIds(),
                            request.getKnowledgeBaseCategories()
                    );
                } else {
                    // 没有提供知识库ID，使用普通流程
                    sessionMono = settingGenerationService.startGenerationHybrid(
                            userId,
                            request.getNovelId(),
                            request.getInitialPrompt(),
                            promptTemplateId,
                            request.getModelConfigId(),
                            null,
                            request.getUsePublicTextModel()
                    );
                }
            } else {
                // 使用常规混合流程：文本阶段 + 工具直通（服务端自行管理 textEndSentinel）
                sessionMono = settingGenerationService.startGenerationHybrid(
                        userId,
                        request.getNovelId(),
                        request.getInitialPrompt(),
                        promptTemplateId,
                        request.getModelConfigId(),
                        null,
                        request.getUsePublicTextModel()
                );
            }
            
            return sessionMono.flatMapMany(session -> 
                    // 返回事件流（在完成/不可恢复错误时自动结束SSE）
                    settingGenerationService.getGenerationEventStream(session.getSessionId())
                        // 过滤掉可恢复错误，不让前端看到 GENERATION_ERROR（recoverable=true）
                        .filter(event -> {
                            if (event instanceof com.ainovel.server.domain.model.setting.generation.SettingGenerationEvent.GenerationErrorEvent err) {
                                Boolean recoverable = err.getRecoverable();
                                return recoverable == null || !recoverable;
                            }
                            return true;
                        })
                        .doOnSubscribe(s -> log.info("客户端已订阅设定生成事件: {}", session.getSessionId()))
                        .doOnError(error -> log.error("设定生成事件流出错: sessionId={}", session.getSessionId(), error))
                        .doFinally(signal -> log.info("SSE连接关闭: sessionId={}, signal={}", session.getSessionId(), signal))
                        .map(event -> ServerSentEvent.<SettingGenerationEvent>builder()
                            .id(String.valueOf(System.currentTimeMillis()))
                            .event(event.getClass().getSimpleName())
                            .data(event)
                            .build()
                        )
            );
        })
        .onErrorResume(error -> {
            log.error("启动设定生成失败", error);
            // 发送错误事件
            SettingGenerationEvent.GenerationErrorEvent errorEvent = 
                new SettingGenerationEvent.GenerationErrorEvent();
            errorEvent.setErrorCode("START_FAILED");
            errorEvent.setErrorMessage(error.getMessage());
            errorEvent.setRecoverable(false);
            // 补全必要字段，避免前端解析失败
            try {
                errorEvent.setSessionId("session-error-" + System.currentTimeMillis());
                errorEvent.setTimestamp(java.time.LocalDateTime.now());
            } catch (Exception ignore) {}
            
            // 显式发送complete事件（标准负载），确保前端SSE客户端立即关闭连接
            @SuppressWarnings({"rawtypes","unchecked"})
            ServerSentEvent<SettingGenerationEvent> completeSse = (ServerSentEvent<SettingGenerationEvent>)(ServerSentEvent) ServerSentEvent.builder()
                .event("complete")
                .data(java.util.Map.of("data", "[DONE]"))
                .build();

            return Flux.just(
                ServerSentEvent.<SettingGenerationEvent>builder()
                    .event("GenerationErrorEvent")
                    .data(errorEvent)
                    .build(),
                completeSse
            );
        });
    }
    
    /**
     * 从小说设定创建编辑会话
     * 当用户进入小说设定生成页面时调用，支持用户选择编辑模式
     */
    @PostMapping("/novel/{novelId}/edit-session")
    @Operation(summary = "从小说设定创建编辑会话", 
        description = "基于小说现有设定创建编辑会话，支持用户选择创建新快照或编辑上次设定")
    public Mono<ApiResponse<EditSessionResponse>> createEditSessionFromNovel(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "小说ID") @PathVariable String novelId,
            @Valid @RequestBody CreateNovelEditSessionRequest request) {
        
        log.info("Creating edit session from novel {} for user {} with editReason: {} createNewSnapshot: {}", 
            novelId, currentUser.getId(), request.getEditReason(), request.isCreateNewSnapshot());
        
        return settingGenerationService.startSessionFromNovel(
                novelId, 
                currentUser.getId(),
                request.getEditReason(), 
                request.getModelConfigId(),
                request.isCreateNewSnapshot()
            )
            .map(session -> {
                EditSessionResponse response = new EditSessionResponse();
                response.setSessionId(session.getSessionId());
                response.setMessage("编辑会话创建成功");
                response.setHasExistingHistory(session.isFromExistingHistory());
                response.setSnapshotMode((String) session.getMetadata().get("snapshotMode"));
                return ApiResponse.<EditSessionResponse>success(response);
            })
            .onErrorResume(error -> {
                log.error("Failed to create edit session from novel", error);
                return Mono.just(ApiResponse.<EditSessionResponse>error("SESSION_CREATE_FAILED", error.getMessage()));
            });
    }
    
    /**
     * AI修改设定节点
     */
    @PostMapping(value = "/{sessionId}/update-node", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "修改设定节点", 
        description = "修改指定的设定节点及其子节点，返回SSE事件流显示修改过程")
    public Flux<ServerSentEvent<SettingGenerationEvent>> updateNode(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "会话ID") @PathVariable String sessionId,
            @Valid @RequestBody UpdateNodeRequest request) {
        
        log.info("Updating node {} in session {} for user {} with modelConfigId {}, isPublicModel={}, publicModelConfigId={}", 
            request.getNodeId(), sessionId, currentUser.getId(), request.getModelConfigId(), request.getPublicModel(), request.getPublicModelConfigId());
        
        // 周期性心跳，避免长时间无事件导致 HTTP/2 中间层（如 CDN/浏览器）断开连接
        @SuppressWarnings({"rawtypes","unchecked"})
        ServerSentEvent<SettingGenerationEvent> keepAliveSse = (ServerSentEvent<SettingGenerationEvent>)(ServerSentEvent) ServerSentEvent.builder()
            .comment("keepalive")
            .build();
        // 标准 complete 事件，供前端及时收尾（事件名=complete，数据负载与OpenAI风格一致）
        @SuppressWarnings({"rawtypes","unchecked"})
        ServerSentEvent<SettingGenerationEvent> completeSse = (ServerSentEvent<SettingGenerationEvent>)(ServerSentEvent) ServerSentEvent.builder()
            .event("complete")
            .data(java.util.Map.of("data", "[DONE]"))
            .build();

        // 先获取事件流，然后启动修改操作（仅启动一次），并对流进行共享，避免多处订阅导致重复启动
        final AtomicBoolean started = new AtomicBoolean(false);
        Flux<ServerSentEvent<SettingGenerationEvent>> eventSseFlux = settingGenerationService.getModificationEventStream(sessionId)
            // 与 start 接口对齐：屏蔽可恢复错误（recoverable=true）的 GENERATION_ERROR 事件
            .filter(event -> {
                if (event instanceof SettingGenerationEvent.GenerationErrorEvent err) {
                    Boolean recoverable = err.getRecoverable();
                    return recoverable == null || !recoverable;
                }
                return true;
            })
            .doOnSubscribe(subscription -> {
                if (started.compareAndSet(false, true)) {
                    settingGenerationService.modifyNode(
                        sessionId,
                        request.getNodeId(),
                        request.getModificationPrompt(),
                        request.getModelConfigId(),
                        request.getScope() == null ? "self" : request.getScope(),
                        request.getPublicModel(),
                        request.getPublicModelConfigId()
                    ).subscribe(
                        result -> log.info("Node modification completed for session: {}", sessionId),
                        error -> log.error("Node modification failed for session: {}", sessionId, error)
                    );
                } else {
                    log.debug("update-node stream already started for session: {}", sessionId);
                }
            })
            .takeUntil(event -> {
                if (event instanceof SettingGenerationEvent.GenerationCompletedEvent) {
                    return true; // 修改流程完成，结束流
                }
                if (event instanceof SettingGenerationEvent.GenerationErrorEvent err) {
                    return err.getRecoverable() != null && !err.getRecoverable(); // 不可恢复错误，结束流
                }
                return false;
            })
            .map(event -> ServerSentEvent.<SettingGenerationEvent>builder()
                .id(String.valueOf(System.currentTimeMillis()))
                .event(event.getClass().getSimpleName())
                .data(event)
                .build()
            )
            // 共享上游订阅，避免 heartbeat 与主流各自订阅导致重复启动
            .publish()
            .refCount(1)
            .onErrorResume(error -> {
                log.error("Failed to update node", error);
                SettingGenerationEvent.GenerationErrorEvent errorEvent = 
                    new SettingGenerationEvent.GenerationErrorEvent();
                errorEvent.setSessionId(sessionId);
                errorEvent.setErrorCode("UPDATE_FAILED");
                errorEvent.setErrorMessage(error.getMessage());
                errorEvent.setNodeId(request.getNodeId());
                errorEvent.setRecoverable(false);
                ServerSentEvent<SettingGenerationEvent> errorSse = ServerSentEvent.<SettingGenerationEvent>builder()
                    .event("GenerationErrorEvent")
                    .data(errorEvent)
                    .build();
                // 错误时也返回 complete，确保前端及时收尾
                return Flux.just(errorSse, completeSse);
            });

        // 15s 心跳流（仅注释行，不携带数据），跟随事件流完成
        Flux<ServerSentEvent<SettingGenerationEvent>> heartbeatFlux = Flux
            .interval(java.time.Duration.ofSeconds(15))
            .map(tick -> keepAliveSse)
            // 事件流完成（正常完成或错误）时，心跳自动结束
            .takeUntilOther(eventSseFlux.ignoreElements().then(Mono.just("stop")));

        // 合并实际事件与心跳，并在业务完成后显式拼接 complete
        return Flux.merge(eventSseFlux, heartbeatFlux)
            .concatWith(Mono.just(completeSse));
    }
    
    /**
     * 直接更新节点内容
     */
    @PostMapping("/{sessionId}/update-content")
    @Operation(summary = "直接更新节点内容", 
        description = "直接更新指定节点的内容，不通过AI重新生成")
    public Mono<ApiResponse<String>> updateNodeContent(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "会话ID") @PathVariable String sessionId,
            @Valid @RequestBody UpdateNodeContentRequest request) {
        
        log.info("Updating node content {} in session {} for user {}", 
            request.getNodeId(), sessionId, currentUser.getId());
        
        return settingGenerationService.updateNodeContent(
                sessionId, 
                request.getNodeId(), 
                request.getNewContent()
            )
            .then(Mono.just(ApiResponse.success("节点内容已更新")))
            .onErrorResume(error -> {
                log.error("Failed to update node content", error);
                return Mono.just(ApiResponse.error("UPDATE_CONTENT_FAILED", "更新节点内容失败: " + error.getMessage()));
            });
    }
    
    /**
     * 删除节点（包括所有子节点）
     */
    @DeleteMapping("/{sessionId}/nodes/{nodeId}")
    @Operation(summary = "删除节点", 
        description = "删除指定节点及其所有子节点")
    public Mono<ApiResponse<DeleteNodeResponse>> deleteNode(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "会话ID") @PathVariable String sessionId,
            @Parameter(description = "节点ID") @PathVariable String nodeId) {
        
        log.info("Deleting node {} from session {} for user {}", 
            nodeId, sessionId, currentUser.getId());
        
        return settingGenerationService.deleteNode(sessionId, nodeId)
            .map(deletedIds -> {
                DeleteNodeResponse response = new DeleteNodeResponse();
                response.setNodeId(nodeId);
                response.setDeletedNodeIds(deletedIds);
                response.setMessage(String.format("成功删除节点及其 %d 个子节点", deletedIds.size()));
                return ApiResponse.success(response);
            })
            .onErrorResume(error -> {
                log.error("Failed to delete node", error);
                return Mono.just(ApiResponse.error("DELETE_NODE_FAILED", "删除节点失败: " + error.getMessage()));
            });
    }
    
    /**
     * 保存生成的设定
     * 保存完成后会自动创建历史记录
     */
    @PostMapping("/{sessionId}/save")
    @Operation(summary = "保存生成的设定", 
        description = "将会话中的设定保存到数据库，并自动创建历史记录快照")
    public Mono<ApiResponse<SaveSettingResponse>> saveGeneratedSettings(
            @RequestHeader(value = "X-User-Id", required = false) String userId,
            @Parameter(description = "会话ID") @PathVariable String sessionId,
            @Valid @RequestBody SaveSettingsRequest request) {

        // 🔧 修复：为开发环境提供默认用户ID
        final String finalUserId = (userId == null || userId.trim().isEmpty()) 
            ? "67d67d6833335f5166782e6f" // 默认测试用户ID
            : userId;
        
        if (userId == null || userId.trim().isEmpty()) {
            log.warn("使用默认用户ID进行保存操作: {}", finalUserId);
        }

        log.info("Saving generated settings for session {} to novel {} by user {}, updateExisting: {}, targetHistoryId: {}", 
                sessionId, request.getNovelId(), finalUserId, request.getUpdateExisting(), request.getTargetHistoryId());

        // 根据请求参数调用相应的保存方法
        boolean updateExisting = Boolean.TRUE.equals(request.getUpdateExisting());
        String targetHistoryId = updateExisting ? request.getTargetHistoryId() : null;
        
        // 如果是更新现有历史记录但没有提供targetHistoryId，则使用sessionId作为默认值
        if (updateExisting && (targetHistoryId == null || targetHistoryId.trim().isEmpty())) {
            targetHistoryId = sessionId;
            log.info("使用sessionId作为默认的targetHistoryId: {}", targetHistoryId);
        }

        return settingGenerationService.saveGeneratedSettings(sessionId, request.getNovelId(), updateExisting, targetHistoryId)
            .map(saveRes -> {
                // Service 已自动创建历史记录，这里仅构造响应
                SaveSettingResponse response = new SaveSettingResponse();
                response.setSuccess(true);
                response.setMessage("设定已成功保存，并已创建历史记录");
                response.setRootSettingIds(saveRes.getRootSettingIds());
                response.setHistoryId(saveRes.getHistoryId());
                return ApiResponse.success(response);
            })
            .onErrorResume(error -> {
                log.error("Failed to save settings", error);
                SaveSettingResponse response = new SaveSettingResponse();
                response.setSuccess(false);
                response.setMessage("保存失败: " + error.getMessage());
                return Mono.just(ApiResponse.error("SAVE_FAILED", error.getMessage()));
            });
    }

    /**
     * 基于会话整体调整生成
     * 使用已存在会话中的设定树与初始提示词进行整体调整，返回生成过程的SSE事件流
     */
    @PostMapping(value = "/{sessionId}/adjust", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "整体调整生成",
        description = "在不破坏现有层级与关联关系的前提下，基于当前会话进行整体调整生成，返回SSE事件流")
    public Flux<ServerSentEvent<SettingGenerationEvent>> adjustSession(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "会话ID") @PathVariable String sessionId,
            @Valid @RequestBody AdjustSessionRequest request) {

        log.info("Adjusting session {} for user {} with modelConfigId {}", sessionId, currentUser.getId(), request.getModelConfigId());

        // 提示词增强：明确保持层级/关联结构，避免UUID等无意义ID
        final String enhancedPrompt =
                "请在不破坏现有层级结构与父子关联关系的前提下，对设定进行整体调整。" +
                "保留节点的层级与引用关系（使用名称/路径表达），避免包含任何UUID或无意义的内部ID。" +
                "\n调整说明：\n" + request.getAdjustmentPrompt();

        // 显式追加完成事件，确保前端能立即关闭SSE连接
        @SuppressWarnings({"rawtypes","unchecked"})
        ServerSentEvent<SettingGenerationEvent> completeSse = (ServerSentEvent<SettingGenerationEvent>)(ServerSentEvent) ServerSentEvent.builder()
                .event("complete")
                .data(java.util.Map.of("data", "[DONE]"))
                .build();

        // 先返回事件流，再在订阅后触发调整操作，避免竞态
        return settingGenerationService.getGenerationEventStream(sessionId)
                .doOnSubscribe(subscription -> {
                    settingGenerationService.adjustSession(
                            sessionId,
                            enhancedPrompt,
                            request.getModelConfigId(),
                            request.getPromptTemplateId()
                    ).subscribe(
                            result -> log.info("Session adjustment completed for session: {}", sessionId),
                            error -> log.error("Session adjustment failed for session: {}", sessionId, error)
                    );
                })
                .takeUntil(event -> {
                    if (event instanceof SettingGenerationEvent.GenerationCompletedEvent) {
                        return true; // 调整完成，结束流
                    }
                    if (event instanceof SettingGenerationEvent.GenerationErrorEvent err) {
                        return err.getRecoverable() != null && !err.getRecoverable(); // 不可恢复错误，结束流
                    }
                    return false;
                })
                .map(event -> ServerSentEvent.<SettingGenerationEvent>builder()
                        .id(String.valueOf(System.currentTimeMillis()))
                        .event(event.getClass().getSimpleName())
                        .data(event)
                        .build()
                )
                // 正常完成时，追加一个标准complete事件
                .concatWith(Mono.just(completeSse))
                .onErrorResume(error -> {
                    log.error("Failed to adjust session", error);
                    SettingGenerationEvent.GenerationErrorEvent errorEvent = new SettingGenerationEvent.GenerationErrorEvent();
                    errorEvent.setSessionId(sessionId);
                    errorEvent.setErrorCode("ADJUST_FAILED");
                    errorEvent.setErrorMessage(error.getMessage());
                    errorEvent.setRecoverable(true);
                    ServerSentEvent<SettingGenerationEvent> errorSse = ServerSentEvent.<SettingGenerationEvent>builder()
                            .event("GenerationErrorEvent")
                            .data(errorEvent)
                            .build();
                    // 错误时也追加complete，确保前端及时关闭SSE
                    return Flux.just(errorSse, completeSse);
                });
    }

    /**
     * 开始写作：确保novelId存在，保存当前session的设定到小说，并将小说标记为未就绪→就绪，返回小说ID
     *
     * 语义调整：彻底忽略历史记录的 novelId。历史仅作为设定树来源，不参与 novelId 的确定。
     *
     * 新增参数：
     * - fork: Boolean，默认 true（表示创建新小说，不复用会话里的 novelId）
     * - reuseNovel: Boolean（保留解析，不再使用历史记录 novelId）
     * 说明：当 fork 与 reuseNovel 同时传入时，以 fork 为准（fork=true 则强制新建）。
     */
    @PostMapping("/start-writing")
    @Operation(summary = "开始写作", description = "确保novelId存在，保存当前会话设定并关联到小说，然后返回小说ID")
    public Mono<ApiResponse<Map<String, String>>> startWriting(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @RequestHeader(value = "X-User-Id", required = false) String headerUserId,
            @RequestBody Map<String, String> body
    ) {
        String sessionId = body.get("sessionId");
        String novelId = body.get("novelId");
        String historyId = body.get("historyId");

        // 解析 fork / reuseNovel 标志（默认创建新小说：fork=true）
        boolean fork = parseBoolean(body.get("fork")).orElse(true);
        parseBoolean(body.get("reuseNovel")).orElse(false); // 保留解析，逻辑已并入优先级顺序

        // 日志：入口参数与语义声明
        try {
            log.info("[开始写作] 忽略历史记录的 novelId，仅用于设定树：sessionId={}, body.novelId={}, historyId={}, fork={}",
                    sessionId, novelId, historyId, fork);
        } catch (Exception ignore) {}

        // 1) novelId / session 优先；其后 fork；否则新建（忽略历史记录 novelId）
        Mono<String> ensureNovel = Mono.defer(() -> {
            // 显式 novelId 优先
            if (novelId != null && !novelId.isBlank()) {
                try { log.info("[开始写作] 使用请求体提供的 novelId: {}", novelId); } catch (Exception ignore) {}
                return Mono.just(novelId);
            }
            // 会话中的 novelId 次之
            if (sessionId != null && !sessionId.isBlank()) {
                Mono<String> fromSession = sessionManager.getSession(sessionId)
                        .flatMap(sess -> {
                            String id = sess.getNovelId();
                            if (id != null && !id.isBlank()) {
                                try { log.info("[开始写作] 使用会话中的 novelId: {} (sessionId={})", id, sessionId); } catch (Exception ignore) {}
                            }
                            return (id == null || id.isBlank()) ? reactor.core.publisher.Mono.empty() : reactor.core.publisher.Mono.just(id);
                        });
                return fromSession.switchIfEmpty(Mono.defer(() -> {
                    // 若会话没有 novelId，则根据 fork 判断；不再从历史记录派生 novelId
                    if (fork) {
                        try { log.info("[开始写作] 会话无 novelId，fork=true → 创建草稿小说"); } catch (Exception ignore) {}
                        return novelService.createNovel(Novel.builder()
                                .title("未命名小说")
                                .description("自动创建的草稿，用于写作编排")
                                .author(Novel.Author.builder().id(currentUser.getId()).username(currentUser.getUsername()).build())
                                .isReady(true)
                                .build()).map(Novel::getId);
                    }
                    // fork=false 也不再使用历史记录 novelId，直接新建
                    try { log.info("[开始写作] 会话无 novelId，fork=false → 仍然创建草稿小说"); } catch (Exception ignore) {}
                    return novelService.createNovel(Novel.builder()
                            .title("未命名小说")
                            .description("自动创建的草稿，用于写作编排")
                            .author(Novel.Author.builder().id(currentUser.getId()).username(currentUser.getUsername()).build())
                            .isReady(true)
                            .build()).map(Novel::getId);
                }));
            }
            // 无 sessionId：按 fork 决定
            if (fork) {
                try { log.info("[开始写作] 无 sessionId，fork=true → 创建草稿小说"); } catch (Exception ignore) {}
                return novelService.createNovel(Novel.builder()
                        .title("未命名小说")
                        .description("自动创建的草稿，用于写作编排")
                        .author(Novel.Author.builder().id(currentUser.getId()).username(currentUser.getUsername()).build())
                        .isReady(true)
                        .build()).map(Novel::getId);
            }
            // fork=false 且未提供 novelId / session.novelId：直接新建（不再参考历史记录 novelId）
            try { log.info("[开始写作] 无 sessionId，fork=false → 创建草稿小说"); } catch (Exception ignore) {}
            return novelService.createNovel(Novel.builder()
                    .title("未命名小说")
                    .description("自动创建的草稿，用于写作编排")
                    .author(Novel.Author.builder().id(currentUser.getId()).username(currentUser.getUsername()).build())
                    .isReady(true)
                    .build()).map(Novel::getId);
        });

        String effectiveUserId = (currentUser != null && currentUser.getId() != null && !currentUser.getId().isBlank())
                ? currentUser.getId() : (headerUserId != null ? headerUserId : null);
        String effectiveUsername = (currentUser != null && currentUser.getUsername() != null && !currentUser.getUsername().isBlank())
                ? currentUser.getUsername() : effectiveUserId;
        if (effectiveUserId == null || effectiveUserId.isBlank()) {
            return Mono.just(ApiResponse.error("UNAUTHORIZED", "START_WRITING_FAILED"));
        }
        // 统一使用 ensureNovel 的结果作为本次写作流程的 novelId，避免出现前后不一致
        return ensureNovel
                .flatMap(ensuredNovelId -> settingComposeService
                        .orchestrateStartWriting(effectiveUserId, effectiveUsername, sessionId, ensuredNovelId, historyId)
                        .map(nid -> ApiResponse.success(Map.of("novelId", nid)))
                        .onErrorResume(e -> {
                            String msg = e.getMessage() != null ? e.getMessage() : "发生未知错误";
                            if (e instanceof IllegalStateException && msg.startsWith("Session not completed")) {
                                return Mono.just(ApiResponse.error("会话未完成，请等待生成完成后再开始写作，或传入historyId", "SESSION_NOT_COMPLETED"));
                            }
                            // 容错：若误将 sessionId 当作 historyId 导致“历史记录不存在”，
                            // 依然返回成功并带上已确保的 novelId，避免前端因格式化错误文本而判失败
                            if (msg.startsWith("历史记录不存在")) {
                                return Mono.just(ApiResponse.success(Map.of("novelId", ensuredNovelId)));
                            }
                            return Mono.just(ApiResponse.error(msg, "START_WRITING_FAILED"));
                        })
                );
    }

    private java.util.Optional<Boolean> parseBoolean(Object val) {
        if (val == null) return java.util.Optional.empty();
        if (val instanceof Boolean b) return java.util.Optional.of(b);
        if (val instanceof String s) {
            String t = s.trim().toLowerCase();
            if ("true".equals(t) || "1".equals(t) || "yes".equals(t) || "y".equals(t)) return java.util.Optional.of(Boolean.TRUE);
            if ("false".equals(t) || "0".equals(t) || "no".equals(t) || "n".equals(t)) return java.util.Optional.of(Boolean.FALSE);
        }
        return java.util.Optional.empty();
    }

    /**
     * 轻量状态查询：仅报告是否存在该会话或历史记录
     */
    @GetMapping("/status-lite/{id}")
    @Operation(summary = "轻量状态查询", description = "返回ID是否为有效的会话或历史记录")
    public Mono<ApiResponse<Map<String, Object>>> getStatusLite(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "会话ID或历史记录ID") @PathVariable String id) {
        return settingComposeService.getStatusLite(id).map(ApiResponse::success);
    }

    /**
     * 获取会话状态
     */
        @GetMapping("/{sessionId}/status")
        @Operation(summary = "获取会话状态", description = "获取指定会话的当前状态信息")
        public Mono<ApiResponse<SessionStatusResponse>> getSessionStatus(
                @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
                @Parameter(description = "会话ID") @PathVariable String sessionId) {
            
            log.info("Getting session status {} for user {}", sessionId, currentUser.getId());
            
            return settingGenerationService.getSessionStatus(sessionId)
                .map(status -> {
                    SessionStatusResponse response = new SessionStatusResponse();
                    response.setSessionId(sessionId);
                    response.setStatus(status.status());
                    response.setProgress(status.progress());
                    response.setCurrentStep(status.currentStep());
                    response.setTotalSteps(status.totalSteps());
                    response.setErrorMessage(status.errorMessage());
                    return ApiResponse.<SessionStatusResponse>success(response);
                })
                .onErrorResume(error -> {
                    log.error("Failed to get session status", error);
                    return Mono.just(ApiResponse.<SessionStatusResponse>error("STATUS_GET_FAILED", error.getMessage()));
                });
        }

    /**
     * 取消生成会话
     */
    @PostMapping("/{sessionId}/cancel")
    @Operation(summary = "取消生成会话", description = "取消正在进行的设定生成会话")
    public Mono<ApiResponse<String>> cancelSession(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "会话ID") @PathVariable String sessionId) {
        
        log.info("Cancelling session {} for user {}", sessionId, currentUser.getId());
        
        return settingGenerationService.cancelSession(sessionId)
            .then(Mono.just(ApiResponse.success("会话已取消")))
            .onErrorResume(error -> {
                log.error("Failed to cancel session", error);
                return Mono.just(ApiResponse.error("CANCEL_FAILED", "取消会话失败: " + error.getMessage()));
            });
    }
    
    // ==================== 策略管理接口 ====================
    
    /**
     * 创建用户自定义策略
     */
    @PostMapping("/strategies/custom")
    @Operation(summary = "创建用户自定义策略", description = "用户创建完全自定义的设定生成策略")
    public Mono<ApiResponse<StrategyResponse>> createCustomStrategy(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Valid @RequestBody CreateCustomStrategyRequest request) {
        
        // 中文日志
        log.info("创建自定义策略，请求用户: {}, 名称: {}", currentUser != null ? currentUser.getId() : "匿名", request.getName());

        String createdByUserId = currentUser != null ? currentUser.getId() : null;
        
        // 创建模板对象直接保存
        EnhancedUserPromptTemplate template = EnhancedUserPromptTemplate.builder()
            .userId(createdByUserId)
            .featureType(com.ainovel.server.domain.model.AIFeatureType.SETTING_TREE_GENERATION)
            .name(request.getName())
            .description(request.getDescription())
            .systemPrompt(request.getSystemPrompt())
            .userPrompt(request.getUserPrompt())
            .settingGenerationConfig(buildStrategyConfig(request))
            .isPublic(false)
            .hidePrompts(request.getHidePrompts() != null ? request.getHidePrompts() : false)
            .isDefault(false)
            .authorId(createdByUserId)
            .version(1)
            .likeCount(0L)
            .favoriteCount(0L)
            .usageCount(0L)
            .createdAt(java.time.LocalDateTime.now())
            .updatedAt(java.time.LocalDateTime.now())
            .build();
        
        return templateRepository.save(template)
            .map(savedTemplate -> {
                StrategyResponse response = mapToStrategyResponse(savedTemplate);
                log.info("自定义策略创建成功: {}", savedTemplate.getId());
                return ApiResponse.<StrategyResponse>success(response);
            })
            .onErrorResume(error -> {
                log.error("创建自定义策略失败", error);
                return Mono.just(ApiResponse.<StrategyResponse>error("STRATEGY_CREATE_FAILED", error.getMessage()));
            });
    }
    
    /**
     * 基于现有策略创建新策略
     */
    @PostMapping("/strategies/from-base/{baseTemplateId}")
    @Operation(summary = "基于现有策略创建新策略", description = "基于系统预设或其他用户的策略创建个性化策略")
    public Mono<ApiResponse<StrategyResponse>> createStrategyFromBase(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "基础策略模板ID") @PathVariable String baseTemplateId,
            @Valid @RequestBody CreateFromBaseStrategyRequest request) {
        
        log.info("Creating strategy from base {} for user: {}, name: {}", baseTemplateId, currentUser.getId(), request.getName());
        
        return templateRepository.findById(baseTemplateId)
            .switchIfEmpty(Mono.error(new IllegalArgumentException("Base template not found: " + baseTemplateId)))
            .flatMap(baseTemplate -> {
                // 检查权限
                if (!baseTemplate.getIsPublic() && !baseTemplate.getUserId().equals(currentUser.getId())) {
                    return Mono.error(new IllegalArgumentException("No permission to use base template"));
                }
                
                if (!baseTemplate.isSettingGenerationTemplate()) {
                    return Mono.error(new IllegalArgumentException("Base template is not for setting generation"));
                }
                
                // 创建新模板
                EnhancedUserPromptTemplate newTemplate = EnhancedUserPromptTemplate.builder()
                    .userId(currentUser.getId())
                    .featureType(com.ainovel.server.domain.model.AIFeatureType.SETTING_TREE_GENERATION)
                    .name(request.getName())
                    .description(request.getDescription())
                    .systemPrompt(request.getSystemPrompt() != null ? request.getSystemPrompt() : baseTemplate.getSystemPrompt())
                    .userPrompt(request.getUserPrompt() != null ? request.getUserPrompt() : baseTemplate.getUserPrompt())
                    .settingGenerationConfig(baseTemplate.getSettingGenerationConfig()) // 直接使用基础配置
                    .sourceTemplateId(baseTemplateId)
                    .isPublic(false)
                    .isDefault(false)
                    .authorId(currentUser.getId())
                    .version(1)
                    .createdAt(java.time.LocalDateTime.now())
                    .updatedAt(java.time.LocalDateTime.now())
                    .build();
                
                return templateRepository.save(newTemplate);
            })
            .map(template -> {
                StrategyResponse response = mapToStrategyResponse(template);
                log.info("Strategy created from base successfully: {}", template.getId());
                return ApiResponse.<StrategyResponse>success(response);
            })
            .onErrorResume(error -> {
                log.error("Failed to create strategy from base", error);
                return Mono.just(ApiResponse.<StrategyResponse>error("STRATEGY_CREATE_FROM_BASE_FAILED", error.getMessage()));
            });
    }
    
    /**
     * 获取用户的策略列表
     */
    @GetMapping("/strategies/my")
    @Operation(summary = "获取用户的策略列表", description = "获取当前用户创建的所有策略")
    public Flux<StrategyResponse> getUserStrategies(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "页码") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "每页大小") @RequestParam(defaultValue = "20") int size) {
        
        final String currentUserIdForList = (currentUser != null && currentUser.getId() != null) ? currentUser.getId() : "67d67d6833335f5166782e6f";
        log.info("获取用户策略列表: 用户={}, 页码={}, 每页={}", currentUserIdForList, page, size);
        
        return strategyManagementService.getUserStrategies(currentUserIdForList, 
                org.springframework.data.domain.PageRequest.of(page, size))
            .map(this::mapToStrategyResponse)
            .onErrorResume(error -> {
                log.error("Failed to get user strategies", error);
                return Flux.empty();
            });
    }
    
    /**
     * 获取公开策略列表
     */
    @GetMapping("/strategies/public")
    @Operation(summary = "获取公开策略列表", description = "获取所有审核通过的公开策略")
    public Flux<StrategyResponse> getPublicStrategies(
            @Parameter(description = "分类筛选") @RequestParam(required = false) String category,
            @Parameter(description = "页码") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "每页大小") @RequestParam(defaultValue = "20") int size) {
        
        log.info("获取公开策略列表: 分类={}, 页码={}, 每页={}", category, page, size);
        
        return strategyManagementService.getPublicStrategies(category, 
                org.springframework.data.domain.PageRequest.of(page, size))
            .map(this::mapToStrategyResponse)
            .onErrorResume(error -> {
                log.error("Failed to get public strategies", error);
                return Flux.empty();
            });
    }
    
    /**
     * 获取策略详情
     */
    @GetMapping("/strategies/{strategyId}")
    @Operation(summary = "获取策略详情", description = "获取指定策略的详细信息")
    public Mono<ApiResponse<StrategyDetailResponse>> getStrategyDetail(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "策略ID") @PathVariable String strategyId) {
        
        // 中文日志 + 空安全
        final String currentUserId = currentUser != null ? currentUser.getId() : null;
        log.info("获取策略详情: {}, 请求用户: {}", strategyId, currentUserId != null ? currentUserId : "匿名");
        
        return templateRepository.findById(strategyId)
            .switchIfEmpty(Mono.error(new IllegalArgumentException("Strategy not found: " + strategyId)))
            .flatMap(template -> {
                // 兼容旧数据：优先使用 userId，其次使用 authorId
                final String ownerUserId = template.getUserId() != null ? template.getUserId() : template.getAuthorId();
                final boolean isOwner = ownerUserId != null && ownerUserId.equals(currentUserId);
                
                // 检查权限：
                // 1. 作者本人 - 无论什么状态都可以查看
                // 2. 已批准的公开策略 - 所有人可以查看
                // 3. 其他情况 - 不允许查看
                if (!isOwner) {
                    // 不是作者本人，检查是否为公开/已批准的策略
                    boolean isPublicOrApproved = Boolean.TRUE.equals(template.getIsPublic());
                    
                    // 🆕 使用顶层统一的审核状态
                    if (template.getReviewStatus() != null) {
                        // 审核通过的策略可以公开查看
                        isPublicOrApproved = isPublicOrApproved || 
                            ReviewStatusConstants.APPROVED.equals(template.getReviewStatus());
                    }
                    
                    if (!isPublicOrApproved) {
                        log.warn("用户 {} 尝试访问非公开策略 {}，所有者: {}", currentUserId, strategyId, ownerUserId);
                        return Mono.error(new IllegalArgumentException("没有权限查看该策略"));
                    }
                }
                
                if (!template.isSettingGenerationTemplate()) {
                    return Mono.error(new IllegalArgumentException("Template is not for setting generation"));
                }
                
                StrategyDetailResponse response = mapToStrategyDetailResponse(template);
                return Mono.just(ApiResponse.<StrategyDetailResponse>success(response));
            })
            .onErrorResume(error -> {
                log.error("获取策略详情失败", error);
                return Mono.just(ApiResponse.<StrategyDetailResponse>error("STRATEGY_NOT_FOUND", error.getMessage()));
            });
    }
    
    /**
     * 更新策略
     */
    @PutMapping("/strategies/{strategyId}")
    @Operation(summary = "更新策略", description = "更新用户自己创建的策略")
    public Mono<ApiResponse<StrategyResponse>> updateStrategy(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "策略ID") @PathVariable String strategyId,
            @Valid @RequestBody UpdateStrategyRequest request) {
        
        log.info("Updating strategy: {} for user: {}", strategyId, currentUser.getId());
        
        return templateRepository.findByIdAndUserId(strategyId, currentUser.getId())
            .switchIfEmpty(Mono.error(new IllegalArgumentException("Template not found or no permission")))
            .flatMap(template -> {
                if (!template.isSettingGenerationTemplate()) {
                    return Mono.error(new IllegalArgumentException("Template is not for setting generation"));
                }
                
                // 如果策略已经是公开的（审核通过），不允许修改
                if (Boolean.TRUE.equals(template.getIsPublic())) {
                    return Mono.error(new IllegalStateException("Cannot modify published strategy"));
                }
                
                // 更新基本信息
                if (request.getName() != null) {
                    template.setName(request.getName());
                }
                if (request.getDescription() != null) {
                    template.setDescription(request.getDescription());
                }
                if (request.getSystemPrompt() != null) {
                    template.setSystemPrompt(request.getSystemPrompt());
                }
                if (request.getUserPrompt() != null) {
                    template.setUserPrompt(request.getUserPrompt());
                }
                
                // 更新配置
                if (request.getNodeTemplates() != null || request.getExpectedRootNodes() != null || request.getMaxDepth() != null) {
                    com.ainovel.server.domain.model.settinggeneration.SettingGenerationConfig config = template.getSettingGenerationConfig();
                    if (config != null) {
                        com.ainovel.server.domain.model.settinggeneration.SettingGenerationConfig updatedConfig = 
                            com.ainovel.server.domain.model.settinggeneration.SettingGenerationConfig.builder()
                                .strategyName(config.getStrategyName())
                                .description(config.getDescription())
                                .nodeTemplates(request.getNodeTemplates() != null ? request.getNodeTemplates() : config.getNodeTemplates())
                                .expectedRootNodes(request.getExpectedRootNodes() != null ? request.getExpectedRootNodes() : config.getExpectedRootNodes())
                                .maxDepth(request.getMaxDepth() != null ? request.getMaxDepth() : config.getMaxDepth())
                                .rules(config.getRules())
                                .metadata(config.getMetadata())
                                .baseStrategyId(config.getBaseStrategyId())
                                .isSystemStrategy(false)
                                .createdAt(config.getCreatedAt())
                                .updatedAt(java.time.LocalDateTime.now())
                                .build();
                        template.setSettingGenerationConfig(updatedConfig);
                    }
                }
                
                template.setUpdatedAt(java.time.LocalDateTime.now());
                template.setVersion(template.getVersion() + 1);
                
                return templateRepository.save(template);
            })
            .map(template -> {
                StrategyResponse response = mapToStrategyResponse(template);
                log.info("Strategy updated successfully: {}", strategyId);
                return ApiResponse.<StrategyResponse>success(response);
            })
            .onErrorResume(error -> {
                log.error("Failed to update strategy", error);
                return Mono.just(ApiResponse.<StrategyResponse>error("STRATEGY_UPDATE_FAILED", error.getMessage()));
            });
    }
    
    /**
     * 删除策略
     */
    @DeleteMapping("/strategies/{strategyId}")
    @Operation(summary = "删除策略", description = "删除用户自己创建的策略")
    public Mono<ApiResponse<String>> deleteStrategy(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "策略ID") @PathVariable String strategyId) {
        
        log.info("Deleting strategy: {} for user: {}", strategyId, currentUser.getId());
        
        return templateRepository.findByIdAndUserId(strategyId, currentUser.getId())
            .switchIfEmpty(Mono.error(new IllegalArgumentException("Template not found or no permission")))
            .flatMap(template -> {
                if (!template.isSettingGenerationTemplate()) {
                    return Mono.error(new IllegalArgumentException("Template is not for setting generation"));
                }
                
                // 如果策略已经是公开的（审核通过），不允许删除
                if (Boolean.TRUE.equals(template.getIsPublic())) {
                    return Mono.error(new IllegalStateException("Cannot delete published strategy"));
                }
                
                return templateRepository.delete(template);
            })
            .then(Mono.just(ApiResponse.success("策略已删除")))
            .doOnSuccess(response -> log.info("Strategy deleted successfully: {}", strategyId))
            .onErrorResume(error -> {
                log.error("Failed to delete strategy", error);
                return Mono.just(ApiResponse.error("STRATEGY_DELETE_FAILED", error.getMessage()));
            });
    }
    
    /**
     * 点赞策略
     */
    @PostMapping("/strategies/{strategyId}/like")
    @Operation(summary = "点赞策略", description = "为策略点赞或取消点赞")
    public Mono<ApiResponse<Map<String, Object>>> likeStrategy(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "策略ID") @PathVariable String strategyId) {
        
        log.info("Toggle like for strategy: {} by user: {}", strategyId, currentUser.getId());
        
        return templateRepository.findById(strategyId)
            .switchIfEmpty(Mono.error(new IllegalArgumentException("Strategy not found")))
            .flatMap(template -> {
                boolean isLiked = Boolean.TRUE.equals(template.getIsLiked());
                
                if (isLiked) {
                    template.decrementLikeCount();
                    template.setIsLiked(false);
                } else {
                    template.incrementLikeCount();
                    template.setIsLiked(true);
                }
                
                return templateRepository.save(template);
            })
            .map(template -> {
                Map<String, Object> result = new HashMap<>();
                result.put("isLiked", template.getIsLiked());
                result.put("likeCount", template.getLikeCount());
                return ApiResponse.success(result);
            })
            .onErrorResume(error -> {
                log.error("Failed to toggle like", error);
                return Mono.just(ApiResponse.error("LIKE_FAILED", error.getMessage()));
            });
    }
    
    /**
     * 收藏策略
     */
    @PostMapping("/strategies/{strategyId}/favorite")
    @Operation(summary = "收藏策略", description = "收藏或取消收藏策略")
    public Mono<ApiResponse<Map<String, Object>>> favoriteStrategy(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "策略ID") @PathVariable String strategyId) {
        
        log.info("Toggle favorite for strategy: {} by user: {}", strategyId, currentUser.getId());
        
        return templateRepository.findById(strategyId)
            .switchIfEmpty(Mono.error(new IllegalArgumentException("Strategy not found")))
            .flatMap(template -> {
                boolean isFavorite = Boolean.TRUE.equals(template.getIsFavorite());
                
                if (isFavorite) {
                    template.decrementFavoriteCount();
                    template.setIsFavorite(false);
                } else {
                    template.incrementFavoriteCount();
                    template.setIsFavorite(true);
                }
                
                return templateRepository.save(template);
            })
            .map(template -> {
                Map<String, Object> result = new HashMap<>();
                result.put("isFavorite", template.getIsFavorite());
                result.put("favoriteCount", template.getFavoriteCount());
                return ApiResponse.success(result);
            })
            .onErrorResume(error -> {
                log.error("Failed to toggle favorite", error);
                return Mono.just(ApiResponse.error("FAVORITE_FAILED", error.getMessage()));
            });
    }
    
    /**
     * 提交策略审核
     */
    @PostMapping("/strategies/{strategyId}/submit-review")
    @Operation(summary = "提交策略审核", description = "将策略提交审核以便公开分享")
    public Mono<ApiResponse<String>> submitStrategyForReview(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "策略ID") @PathVariable String strategyId) {
        
        // 检查用户是否已登录
        if (currentUser == null || currentUser.getId() == null) {
            log.warn("未登录用户尝试提交策略审核: {}", strategyId);
            return Mono.just(ApiResponse.error("UNAUTHORIZED", "请先登录"));
        }
        
        log.info("提交策略审核: {} by user: {}", strategyId, currentUser.getId());
        
        return strategyManagementService.submitForReview(strategyId, currentUser.getId())
            .then(Mono.just(ApiResponse.success("策略已提交审核")))
            .onErrorResume(error -> {
                log.error("提交策略审核失败", error);
                return Mono.just(ApiResponse.error("SUBMIT_REVIEW_FAILED", error.getMessage()));
            });
    }
    
    // ==================== 管理员审核接口 ====================
    
    /**
     * 获取待审核策略列表（管理员接口）
     */
    @GetMapping("/admin/strategies/pending")
    @Operation(summary = "获取待审核策略列表", description = "管理员获取所有待审核的策略")
    public Flux<StrategyResponse> getPendingStrategies(
            @Parameter(description = "页码") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "每页大小") @RequestParam(defaultValue = "20") int size) {
        
        log.info("Getting pending strategies for review, page: {}, size: {}", page, size);
        
        return strategyManagementService.getPendingReviews(
                org.springframework.data.domain.PageRequest.of(page, size))
            .map(this::mapToStrategyResponse)
            .onErrorResume(error -> {
                log.error("Failed to get pending strategies", error);
                return Flux.empty();
            });
    }
    
    /**
     * 审核策略（管理员接口）
     */
    @PostMapping("/admin/strategies/{strategyId}/review")
    @Operation(summary = "审核策略", description = "管理员审核策略，决定是否通过")
    public Mono<ApiResponse<String>> reviewStrategy(
            @AuthenticationPrincipal com.ainovel.server.security.CurrentUser currentUser,
            @Parameter(description = "策略ID") @PathVariable String strategyId,
            @Valid @RequestBody ReviewStrategyRequest request) {
        
        log.info("Reviewing strategy: {} by reviewer: {}, decision: {}", 
            strategyId, currentUser.getId(), request.getDecision());
        
        // TODO: 实现策略审核的完整逻辑
        return Mono.just(new EnhancedUserPromptTemplate())
            .then(Mono.just(ApiResponse.success("审核完成")))
            .onErrorResume(error -> {
                log.error("Failed to review strategy", error);
                return Mono.just(ApiResponse.error("REVIEW_FAILED", error.getMessage()));
            });
    }
    
    // ==================== 辅助方法 ====================
    
    // 暂时使用简化的映射，后续需要实现完整的服务层方法
    // 这些方法需要根据实际的服务层接口来完善
    
    private StrategyResponse mapToStrategyResponse(EnhancedUserPromptTemplate template) {
        StrategyResponse response = new StrategyResponse();
        
        // 安全地获取各个字段，避免空指针异常，确保所有 String 字段都不为 null
        response.setId(template.getId() != null ? template.getId() : "");
        response.setName(template.getName() != null ? template.getName() : "");
        response.setDescription(template.getDescription() != null ? template.getDescription() : "");
        response.setAuthorId(template.getAuthorId() != null ? template.getAuthorId() : "");
        response.setIsPublic(template.getIsPublic() != null ? template.getIsPublic() : false);
        response.setHidePrompts(template.getHidePrompts() != null ? template.getHidePrompts() : false);
        response.setCreatedAt(template.getCreatedAt());
        response.setUpdatedAt(template.getUpdatedAt());
        response.setUsageCount(template.getUsageCount() != null ? template.getUsageCount() : 0L);
        response.setLikeCount(template.getLikeCount() != null ? template.getLikeCount() : 0L);
        response.setFavoriteCount(template.getFavoriteCount() != null ? template.getFavoriteCount() : 0L);
        response.setIsLiked(template.getIsLiked() != null ? template.getIsLiked() : false);
        response.setIsFavorite(template.getIsFavorite() != null ? template.getIsFavorite() : false);
        response.setRating(template.getRating() != null ? template.getRating() : 0.0);
        
        if (template.getSettingGenerationConfig() != null) {
            response.setExpectedRootNodes(template.getSettingGenerationConfig().getExpectedRootNodes() != null ? 
                template.getSettingGenerationConfig().getExpectedRootNodes() : 8);
            response.setMaxDepth(template.getSettingGenerationConfig().getMaxDepth() != null ? 
                template.getSettingGenerationConfig().getMaxDepth() : 3);
            
            // 🆕 使用顶层统一的审核状态
            if (template.getReviewStatus() != null) {
                response.setReviewStatus(template.getReviewStatus());
            } else {
                response.setReviewStatus(ReviewStatusConstants.DRAFT);
            }
            
            if (template.getSettingGenerationConfig().getMetadata() != null) {
                response.setCategories(template.getSettingGenerationConfig().getMetadata().getCategories());
                response.setTags(template.getSettingGenerationConfig().getMetadata().getTags());
                response.setDifficultyLevel(template.getSettingGenerationConfig().getMetadata().getDifficultyLevel());
            }
        } else {
            // 设置默认值，确保所有必需字段都有值
            response.setExpectedRootNodes(8);
            response.setMaxDepth(3);
            response.setReviewStatus("DRAFT");
        }
        
        return response;
    }
    
    private com.ainovel.server.domain.model.settinggeneration.SettingGenerationConfig buildStrategyConfig(CreateCustomStrategyRequest request) {
        return com.ainovel.server.domain.model.settinggeneration.SettingGenerationConfig.builder()
            .strategyName(request.getName())
            .description(request.getDescription())
            .nodeTemplates(request.getNodeTemplates())
            .expectedRootNodes(request.getExpectedRootNodes())
            .maxDepth(request.getMaxDepth())
            .baseStrategyId(request.getBaseStrategyId())
            .isSystemStrategy(false)
            .createdAt(java.time.LocalDateTime.now())
            .updatedAt(java.time.LocalDateTime.now())
            .build();
    }
    
    private void applyHidePromptsFromRequest(EnhancedUserPromptTemplate template, CreateCustomStrategyRequest request) {
        if (request.getHidePrompts() != null) {
            template.setHidePrompts(request.getHidePrompts());
        }
    }
    
    private StrategyDetailResponse mapToStrategyDetailResponse(EnhancedUserPromptTemplate template) {
        StrategyDetailResponse response = new StrategyDetailResponse();
        
        // 基本信息
        response.setId(template.getId() != null ? template.getId() : "");
        response.setName(template.getName() != null ? template.getName() : "");
        response.setDescription(template.getDescription() != null ? template.getDescription() : "");
        response.setAuthorId(template.getAuthorId() != null ? template.getAuthorId() : "");
        response.setAuthorName(template.getAuthorId() != null ? template.getAuthorId() : ""); // TODO: 可以从User服务获取真实用户名
        response.setIsPublic(template.getIsPublic() != null ? template.getIsPublic() : false);
        response.setHidePrompts(template.getHidePrompts() != null ? template.getHidePrompts() : false);
        response.setCreatedAt(template.getCreatedAt());
        response.setUpdatedAt(template.getUpdatedAt());
        response.setUsageCount(template.getUsageCount() != null ? template.getUsageCount() : 0L);
        response.setLikeCount(template.getLikeCount() != null ? template.getLikeCount() : 0L);
        response.setFavoriteCount(template.getFavoriteCount() != null ? template.getFavoriteCount() : 0L);
        response.setIsLiked(template.getIsLiked() != null ? template.getIsLiked() : false);
        response.setIsFavorite(template.getIsFavorite() != null ? template.getIsFavorite() : false);
        response.setRating(template.getRating());
        
        // 提示词（如果隐藏提示词，则不返回）
        if (!Boolean.TRUE.equals(template.getHidePrompts())) {
            response.setSystemPrompt(template.getSystemPrompt());
            response.setUserPrompt(template.getUserPrompt());
        } else {
            response.setSystemPrompt("***隐藏***");
            response.setUserPrompt("***隐藏***");
        }
        
        // 配置信息
        if (template.getSettingGenerationConfig() != null) {
            response.setExpectedRootNodes(template.getSettingGenerationConfig().getExpectedRootNodes());
            response.setMaxDepth(template.getSettingGenerationConfig().getMaxDepth());
            response.setNodeTemplates(template.getSettingGenerationConfig().getNodeTemplates());
            
            // 🆕 使用顶层统一的审核状态
            if (template.getReviewStatus() != null) {
                response.setReviewStatus(template.getReviewStatus());
            } else {
                response.setReviewStatus(ReviewStatusConstants.DRAFT);
            }
            
            if (template.getSettingGenerationConfig().getMetadata() != null) {
                response.setCategories(template.getSettingGenerationConfig().getMetadata().getCategories());
                response.setTags(template.getSettingGenerationConfig().getMetadata().getTags());
                response.setDifficultyLevel(template.getSettingGenerationConfig().getMetadata().getDifficultyLevel());
            }
        } else {
            response.setExpectedRootNodes(0);
            response.setMaxDepth(5);
            response.setReviewStatus("DRAFT");
        }
        
        return response;
    }
    
    // ==================== DTO 类 ====================
    
    /**
     * 启动生成请求
     */
    @Data
    public static class StartGenerationRequest {
        // 🔧 前端生成的sessionId（可选，如果为空则后端自动生成）
        private String sessionId;
        
        // 📚 复用模式下可以为空，所以移除 @NotBlank 验证，在 isValid() 中进行条件验证
        private String initialPrompt;
        
        // 新的字段，与strategy二选一
        private String promptTemplateId;
        
        private String novelId; // 改为可选
        
        @NotBlank(message = "模型配置ID不能为空")
        private String modelConfigId;
        
        // 当没有JWT认证时使用的用户ID
        private String userId;
        
        // 保留兼容性，与promptTemplateId二选一
        @Deprecated
        private String strategy;

        // 文本阶段是否改用公共模型
        private Boolean usePublicTextModel;
        
        // 📚 知识库集成模式 ('NONE', 'REUSE', 'IMITATION', 'HYBRID')
        private String knowledgeBaseMode;
        
        // 📚 知识库ID列表（用于REUSE和IMITATION模式）
        private List<String> knowledgeBaseIds;
        
        // 📚 知识库分类列表（每个知识库对应一个分类列表）
        // key: knowledgeBaseId, value: list of category values
        private Map<String, List<String>> knowledgeBaseCategories;
        
        // 📚 混合模式专用：用于复用的知识库ID列表
        private List<String> reuseKnowledgeBaseIds;
        
        // 📚 混合模式专用：用于参考的知识库ID列表
        private List<String> referenceKnowledgeBaseIds;
        
        // 🔧 结构化输出循环模式：是否使用结构化输出（直接输出JSON，不使用工具调用）
        private Boolean useStructuredOutput;
        
        // 🔧 结构化输出循环模式：最大迭代次数（默认3次）
        private Integer structuredIterations;
        
        // 自定义验证：promptTemplateId和strategy必须提供其中一个
        // 📚 复用模式下不需要提示词
        public boolean isValid() {
            boolean hasStrategy = (promptTemplateId != null && !promptTemplateId.trim().isEmpty()) ||
                                 (strategy != null && !strategy.trim().isEmpty());
            
            // 复用模式只需要策略，不需要提示词
            boolean isReuseMode = "REUSE".equalsIgnoreCase(knowledgeBaseMode);
            if (isReuseMode) {
                return hasStrategy;
            }
            
            // 其他模式需要提示词和策略
            boolean hasPrompt = initialPrompt != null && !initialPrompt.trim().isEmpty();
            return hasPrompt && hasStrategy;
        }
    }

    /**
     * 创建自定义策略请求
     */
    @Data
    public static class CreateCustomStrategyRequest {
        @NotBlank(message = "策略名称不能为空")
        private String name;
        
        @NotBlank(message = "策略描述不能为空")
        private String description;
        
        @NotBlank(message = "系统提示词不能为空")
        private String systemPrompt;
        
        @NotBlank(message = "用户提示词不能为空")
        private String userPrompt;
        
        private List<NodeTemplateConfig> nodeTemplates;
        
        private Integer expectedRootNodes;
        
        private Integer maxDepth;
        
        private String baseStrategyId; // 可选，如果指定则基于该策略
        
        private Boolean hidePrompts; // 是否隐藏提示词
    }
    
    /**
     * 基于现有策略创建请求
     */
    @Data
    public static class CreateFromBaseStrategyRequest {
        @NotBlank(message = "策略名称不能为空")
        private String name;
        
        @NotBlank(message = "策略描述不能为空")
        private String description;
        
        private String systemPrompt; // 可选，不提供则使用基础策略的
        
        private String userPrompt; // 可选，不提供则使用基础策略的
        
        private Map<String, Object> modifications; // 对基础策略的修改
    }
    
    /**
     * 更新策略请求
     */
    @Data
    public static class UpdateStrategyRequest {
        @NotBlank(message = "策略名称不能为空")
        private String name;
        
        @NotBlank(message = "策略描述不能为空")
        private String description;
        
        private String systemPrompt;
        
        private String userPrompt;
        
        private List<NodeTemplateConfig> nodeTemplates;
        
        private Integer expectedRootNodes;
        
        private Integer maxDepth;
    }
    
    /**
     * 审核策略请求
     */
    @Data
    public static class ReviewStrategyRequest {
        @NotBlank(message = "审核决定不能为空")
        private String decision; // APPROVED, REJECTED
        
        private String comment; // 审核评论
        
        private List<String> rejectionReasons; // 拒绝理由
        
        private List<String> improvementSuggestions; // 改进建议
    }
    
    /**
     * 策略响应
     */
    @Data
    public static class StrategyResponse {
        private String id;
        private String name;
        private String description;
        private String authorId;
        private Boolean isPublic;
        private Boolean hidePrompts;
        private java.time.LocalDateTime createdAt;
        private java.time.LocalDateTime updatedAt;
        private Long usageCount;
        private Long likeCount;
        private Long favoriteCount;
        private Boolean isLiked;
        private Boolean isFavorite;
        private Double rating;
        private Integer expectedRootNodes;
        private Integer maxDepth;
        private String reviewStatus;
        private List<String> categories;
        private List<String> tags;
        private Integer difficultyLevel;
    }
    
    /**
     * 策略详情响应
     */
    @Data
    public static class StrategyDetailResponse {
        private String id;
        private String name;
        private String description;
        private String authorId;
        private String authorName;
        private Boolean isPublic;
        private Boolean hidePrompts;
        private java.time.LocalDateTime createdAt;
        private java.time.LocalDateTime updatedAt;
        private Long usageCount;
        private Long likeCount;
        private Long favoriteCount;
        private Boolean isLiked;
        private Boolean isFavorite;
        private Double rating;
        private Integer expectedRootNodes;
        private Integer maxDepth;
        private String reviewStatus;
        private List<String> categories;
        private List<String> tags;
        private Integer difficultyLevel;
        private String systemPrompt;
        private String userPrompt;
        private List<NodeTemplateConfig> nodeTemplates;
    }

    /**
     * 从小说创建编辑会话请求
     */
    @Data
    public static class CreateNovelEditSessionRequest {
        /**
         * 编辑原因/说明
         */
        private String editReason;
        
        /**
         * 模型配置ID
         */
        @NotBlank(message = "模型配置ID不能为空")
        private String modelConfigId;

        /**
         * 是否创建新的快照
         */
        private boolean createNewSnapshot = false;
    }
    
    /**
     * 更新节点请求
     */
    @Data
    public static class UpdateNodeRequest {
        @NotBlank(message = "节点ID不能为空")
        private String nodeId;
        
        @NotBlank(message = "修改提示词不能为空")
        private String modificationPrompt;
        
        @NotBlank(message = "模型配置ID不能为空")
        private String modelConfigId;

        /**
         * 修改范围：self | children_only | self_and_children
         */
        private String scope;

        /**
         * 是否使用公共模型（可选）。若为true，优先使用 publicModelConfigId 分支。
         * 命名为 publicModel 以适配标准布尔JavaBean访问器（getPublicModel）。
         */
        private Boolean publicModel;

        /**
         * 公共模型配置ID（可选）。仅当 isPublicModel=true 时生效。
         */
        private String publicModelConfigId;
    }

    /**
     * 更新节点内容请求
     */
    @Data
    public static class UpdateNodeContentRequest {
        @NotBlank(message = "节点ID不能为空")
        private String nodeId;
        
        @NotBlank(message = "新内容不能为空")
        private String newContent;
    }

    /**
     * 删除节点响应
     */
    @Data
    public static class DeleteNodeResponse {
        private String nodeId;
        private List<String> deletedNodeIds;
        private String message;
    }

    /**
     * 整体调整生成请求
     */
    @Data
    public static class AdjustSessionRequest {
        @NotBlank(message = "调整提示词不能为空")
        private String adjustmentPrompt;

        @NotBlank(message = "模型配置ID不能为空")
        private String modelConfigId;

        /**
         * 提示词模板ID：用于指定策略/提示风格
         */
        @NotBlank(message = "提示词模板ID不能为空")
        private String promptTemplateId;
    }

    /**
     * 保存设定请求
     */
    @Data
    public static class SaveSettingsRequest {
        /**
         * 小说ID
         * 如果为 null 或空字符串，表示保存为独立快照（不关联任何小说）
         */
        private String novelId;
        
        /**
         * 是否更新现有历史记录
         * true: 更新当前历史记录（一般使用sessionId作为historyId）
         * false: 创建新的历史记录（默认行为）
         */
        private Boolean updateExisting = false;
        
        /**
         * 目标历史记录ID
         * 当updateExisting=true时使用，一般情况下就是sessionId
         */
        private String targetHistoryId;
    }
    
    /**
     * 编辑会话响应
     */
    @Data
    public static class EditSessionResponse {
        private String sessionId;
        private String message;
        private boolean hasExistingHistory;
        private String snapshotMode;
    }
    
    /**
     * 保存设定响应
     */
    @Data
    public static class SaveSettingResponse {
        private boolean success;
        private String message;
        private List<String> rootSettingIds;
        private String historyId; // 新增：自动创建的历史记录ID
    }

    /**
     * 会话状态响应
     */
    @Data
    public static class SessionStatusResponse {
        private String sessionId;
        private String status;
        private Integer progress;
        private String currentStep;
        private Integer totalSteps;
        private String errorMessage;
    }
}