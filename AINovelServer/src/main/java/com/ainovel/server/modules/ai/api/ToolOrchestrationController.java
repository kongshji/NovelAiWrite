package com.ainovel.server.web.controller;

import com.ainovel.server.service.ai.orchestration.ToolStreamingOrchestrator;
import com.ainovel.server.service.ai.tools.ToolDefinition;
import com.ainovel.server.service.ai.tools.events.ToolEvent;
import com.ainovel.server.service.setting.generation.tools.TextToSettingsDataTool;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 通用工具编排流式控制器：暴露纯数据工具直通的SSE接口
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/tool-orchestration")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class ToolOrchestrationController {

    private final ToolStreamingOrchestrator orchestrator;
    private final ObjectMapper objectMapper;

    @PostMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> stream(@Valid @RequestBody StartRequest req) {
        String contextId = req.getContextId() != null && !req.getContextId().isBlank() ? req.getContextId() : ("orchestrate-" + UUID.randomUUID());
        log.info("[ToolOrchestration] start stream, contextId={}, provider={}, model={} ", contextId, req.getProvider(), req.getModelName());

        List<ToolDefinition> tools = new ArrayList<>();
        // 动态选择工具：默认 text_to_settings；支持 text_to_setting_tree
        List<String> toolNames = req.getTools();
        if (toolNames == null || toolNames.isEmpty()) {
            tools.add(new TextToSettingsDataTool());
        } else {
            for (String name : toolNames) {
                if ("text_to_settings".equalsIgnoreCase(name)) {
                    tools.add(new TextToSettingsDataTool());
                } else if ("text_to_setting_tree".equalsIgnoreCase(name)) {
                    tools.add(new com.ainovel.server.service.setting.generation.tools.TextToSettingTreeTool());
                } else {
                    // 未知工具名：忽略或记录
                    log.warn("[ToolOrchestration] Unknown tool requested: {}", name);
                }
            }
            if (tools.isEmpty()) {
                tools.add(new TextToSettingsDataTool());
            }
        }

        var options = new ToolStreamingOrchestrator.StartOptions(
                contextId,
                req.getProvider(),
                req.getModelName(),
                req.getApiKey(),
                req.getApiEndpoint(),
                req.getConfig(),
                tools,
                req.getSystemPrompt(),
                req.getUserPrompt(),
                req.getMaxIterations() != null ? req.getMaxIterations() : 20,
                true
        );

        Flux<ToolEvent> flux = orchestrator.startStreaming(options);

        return flux.map(evt -> {
            try {
                String json = objectMapper.writeValueAsString(evt);
                return ServerSentEvent.<String>builder()
                        .id(evt.getContextId() + ":" + evt.getSequence())
                        .event(evt.getEventType())
                        .data(json)
                        .build();
            } catch (Exception e) {
                log.error("Serialize ToolEvent failed", e);
                return ServerSentEvent.<String>builder()
                        .event("error")
                        .data("{\"error\":\"serialize_failed\"}")
                        .build();
            }
        }).delayElements(Duration.ofMillis(25))
        .concatWith(Mono.just(ServerSentEvent.<String>builder()
                .event("complete")
                .data("{\"data\":\"[DONE]\"}")
                .build()));
    }

    @Data
    public static class StartRequest {
        private String contextId;
        private String provider;
        private String modelName;
        private String apiKey;
        private String apiEndpoint;
        private Map<String, String> config;
        private String systemPrompt;
        private String userPrompt; // 建议把阶段一文本作为 userPrompt 传入
        private Integer maxIterations;
        private List<String> tools; // 可选：指定工具名数组
    }
}


