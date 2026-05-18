package com.ainovel.server.web.controller;

import com.ainovel.server.security.CurrentUser;
// import com.ainovel.server.task.event.internal.*; // 不再需要直接引用内部事件类
import com.ainovel.server.task.model.TaskStatus;
import com.ainovel.server.task.service.TaskSubmissionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import com.ainovel.server.task.events.TaskEventPublisher;
import com.ainovel.server.service.JwtService;
import org.springframework.http.codec.ServerSentEvent;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务状态与事件（SSE）控制器
 * 路由前缀（规范）：/api/v1/tasks；兼容旧路径：/api/v1/api/tasks
 */
@Slf4j
@RestController
@RequestMapping({"/api/v1/tasks", "/api/v1/api/tasks"})
@RequiredArgsConstructor
public class TaskStatusController {

    private final TaskSubmissionService taskSubmissionService;
    private final TaskEventPublisher taskEventPublisher;
    private final JwtService jwtService;
    
    // 🔧 SSE连接管理：记录每个用户的活跃连接，防止重复连接
    private final Map<String, Long> activeConnections = new java.util.concurrent.ConcurrentHashMap<>();
    
    // 🔧 限制每个用户的最大并发连接数
    private final Map<String, Integer> userConnectionCount = new java.util.concurrent.ConcurrentHashMap<>();
    private static final int MAX_CONNECTIONS_PER_USER_OLD = 2; // 旧版本：最多2个（严格限制）
    // 新版本：不限制数量（信任客户端的指数退避和重试限制）
    
    // 🔧 记录用户被拒绝的次数，用于异常检测
    private final Map<String, Integer> userRejectionCount = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, Long> userRejectionResetTime = new java.util.concurrent.ConcurrentHashMap<>();
    private static final int MAX_REJECTIONS_BEFORE_LONGER_BLOCK = 5; // 5次拒绝后延长阻断时间
    private static final long REJECTION_RESET_WINDOW_MS = 60000; // 1分钟内的拒绝次数计数窗口

    /**
     * 查询任务状态
     */
    @GetMapping("/{taskId}/status")
    public Mono<ResponseEntity<Object>> getTaskStatus(
            @AuthenticationPrincipal CurrentUser currentUser,
            @PathVariable("taskId") String taskId) {
        final String userId = currentUser != null ? currentUser.getId() : null;
        return taskSubmissionService.getTaskStatus(taskId, userId)
                .map(body -> {
                    try {
                        Object status = (body instanceof Map<?,?>) ? ((Map<?,?>) body).get("status") : null;
                        boolean hasResult = (body instanceof Map<?,?>) && ((Map<?,?>) body).containsKey("result");
                        log.info("[GET STATUS] taskId={} status={} hasResult={}", taskId, status, hasResult);
                    } catch (Throwable ignore) {}
                    return ResponseEntity.ok(body);
                })
                .onErrorResume(e -> {
                    log.error("获取任务状态失败: taskId={} error={}", taskId, e.getMessage(), e);
                    return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                            "taskId", taskId,
                            "error", e.getMessage()
                    )));
                });
    }

    /**
     * 获取用户历史任务列表（支持分页和状态过滤）
     */
    @GetMapping("/list")
    public Mono<ResponseEntity<List<Map<String, Object>>>> getUserTasks(
            @AuthenticationPrincipal CurrentUser currentUser,
            @RequestParam(value = "status", required = false) String statusParam,
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "size", defaultValue = "5") int size) {
        
        final String userId = currentUser != null ? currentUser.getId() : null;
        if (userId == null) {
            return Mono.just(ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(List.of()));
        }
        
        TaskStatus status = null;
        if (statusParam != null && !statusParam.isBlank()) {
            try {
                status = TaskStatus.valueOf(statusParam.toUpperCase());
            } catch (IllegalArgumentException e) {
                log.warn("无效的任务状态参数: {}", statusParam);
                return Mono.just(ResponseEntity.badRequest().body(List.of()));
            }
        }
        
        log.info("获取用户历史任务: userId={}, status={}, page={}, size={}", 
                userId, status, page, size);
        
        return taskSubmissionService.getUserTasks(userId, status, page, size)
                .map(task -> {
                    // 将BackgroundTask转换为前端友好的格式
                    Map<String, Object> taskData = new HashMap<>();
                    taskData.put("taskId", task.getId());
                    taskData.put("taskType", task.getTaskType());
                    taskData.put("type", mapTaskStatusToEventType(task.getStatus()));
                    taskData.put("status", task.getStatus());
                    taskData.put("userId", task.getUserId());
                    taskData.put("parentTaskId", task.getParentTaskId());
                    taskData.put("parameters", task.getParameters());
                    taskData.put("progress", task.getProgress());
                    taskData.put("result", task.getResult());
                    taskData.put("errorInfo", task.getErrorInfo());
                    taskData.put("ts", task.getTimestamps().getUpdatedAt() != null 
                            ? task.getTimestamps().getUpdatedAt().toEpochMilli() 
                            : task.getTimestamps().getCreatedAt().toEpochMilli());
                    
                    return taskData;
                })
                .collectList()
                .map(taskList -> {
                    log.info("成功获取用户历史任务: userId={}, count={}", userId, taskList.size());
                    return ResponseEntity.ok(taskList);
                })
                .onErrorResume(e -> {
                    log.error("获取用户历史任务失败: userId={}, error={}", userId, e.getMessage(), e);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body(List.of()));
                });
    }
    
    /**
     * 将任务状态映射为SSE事件类型
     */
    private String mapTaskStatusToEventType(TaskStatus status) {
        return switch (status) {
            case QUEUED -> "TASK_SUBMITTED";
            case RUNNING -> "TASK_STARTED";
            case COMPLETED -> "TASK_COMPLETED";
            case FAILED, DEAD_LETTER -> "TASK_FAILED";
            case CANCELLED -> "TASK_CANCELLED";
            default -> "TASK_UNKNOWN";
        };
    }

    // 🔧 最低客户端版本要求（格式：major.minor.patch）
    private static final String MIN_CLIENT_VERSION = "1.5.3";
    
    /**
     * 用户任务事件 SSE 流
     * 使用 GET + text/event-stream（标准范式）。
     */
    @GetMapping(path = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Map<String, Object>>> streamTaskEvents(
            @AuthenticationPrincipal CurrentUser currentUser,
            @RequestParam(value = "userId", required = false) String userIdParam,
            @org.springframework.web.bind.annotation.RequestHeader(value = "X-Client-Version", required = false) String clientVersion,
            ServerHttpRequest request) {
        final String userId = (currentUser != null ? currentUser.getId() : null);

        // 🔒 安全检查：如果无法获取userId，拒绝连接，避免泄露其他用户的事件
        if (userId == null || userId.isBlank()) {
            log.error("[SSE REJECT] SSE连接被拒绝：无法获取有效的userId");
            return Flux.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未认证，SSE连接被拒绝"));
        }

        // 可选安全日志：当传入的 userIdParam 与认证用户不一致时，记录并忽略来路参数
        if (userIdParam != null && !userIdParam.isBlank() && !userId.equals(userIdParam)) {
            log.warn("[SSE WARN] 请求参数中的userId({})与认证用户({})不一致，已忽略请求参数", userIdParam, userId);
        }
        
        // 🔧 版本检查：判断客户端是否是新版本（有指数退避保护）
        boolean isNewVersionClient = false;
        if (clientVersion != null && !clientVersion.isBlank()) {
            log.info("[SSE CONNECT] 用户 {} 客户端版本: {}, 最低要求: {}", userId, clientVersion, MIN_CLIENT_VERSION);
            if (isClientVersionTooOld(clientVersion, MIN_CLIENT_VERSION)) {
                log.warn("[SSE VERSION] 用户 {} 客户端版本过旧（当前: {}，要求: {}），发送complete信号终止连接", 
                        userId, clientVersion, MIN_CLIENT_VERSION);
                // 🔧 发送 complete 信号，让前端终止连接并提示用户升级
                return Flux.just(
                    ServerSentEvent.<Map<String, Object>>builder()
                        .event("complete")
                        .data(Map.of(
                            "reason", "CLIENT_VERSION_TOO_OLD",
                            "message", "客户端版本过旧，请刷新页面更新到最新版本",
                            "currentVersion", clientVersion,
                            "minVersion", MIN_CLIENT_VERSION,
                            "data", "[DONE]"
                        ))
                        .retry(java.time.Duration.ofDays(365))
                        .build()
                );
            } else {
                // 版本号 >= MIN_CLIENT_VERSION，认为是新版本客户端
                isNewVersionClient = true;
                log.info("[SSE NEW CLIENT] 用户 {} 使用新版本客户端（{}），信任其指数退避机制，放宽频率限制", 
                        userId, clientVersion);
            }
        } else {
            log.warn("[SSE OLD CLIENT] 用户 {} 未提供客户端版本号（旧版本），将进行严格频率限制", userId);
        }
        
        // 🔧 连接管理：检查并限制同一用户的并发连接数
        long now = System.currentTimeMillis();
        Long lastConnection = activeConnections.get(userId);
        
        // 🔧 只对旧版本客户端进行频率限制（新版本客户端有自己的指数退避和重试限制）
        if (!isNewVersionClient) {
            // 🔧 检查拒绝次数，如果频繁被拒绝，延长阻断时间
            Long lastRejectionResetTime = userRejectionResetTime.get(userId);
            if (lastRejectionResetTime == null || (now - lastRejectionResetTime) > REJECTION_RESET_WINDOW_MS) {
                // 重置计数窗口
                userRejectionCount.put(userId, 0);
                userRejectionResetTime.put(userId, now);
            }
            
            Integer rejectionCount = userRejectionCount.getOrDefault(userId, 0);
            long minIntervalMs = 500; // 默认最小间隔500ms
            
            // 如果频繁被拒绝，延长最小间隔（指数退避）
            if (rejectionCount >= MAX_REJECTIONS_BEFORE_LONGER_BLOCK) {
                minIntervalMs = 5000; // 延长到5秒
                log.warn("[SSE THROTTLE] 旧版本用户 {} 被拒绝次数过多({}次)，延长阻断时间至{}ms", 
                        userId, rejectionCount, minIntervalMs);
            } else if (rejectionCount >= 3) {
                minIntervalMs = 2000; // 延长到2秒
            }
            
            // 🚫 防止连接风暴：拒绝高频重连（仅针对旧版本）
            if (lastConnection != null && (now - lastConnection) < minIntervalMs) {
                long timeSinceLastConnection = now - lastConnection;
                userRejectionCount.compute(userId, (k, v) -> (v == null ? 1 : v + 1));
                log.warn("[SSE REJECT] 旧版本用户 {} 连接过于频繁（{}ms内重连，需间隔{}ms），疑似连接风暴，拒绝本次连接（累计拒绝{}次）", 
                        userId, timeSinceLastConnection, minIntervalMs, userRejectionCount.get(userId));
                return Flux.error(new IllegalStateException(
                    String.format("连接过于频繁（需间隔至少%dms），请稍后重试。建议：刷新页面以获取最新版本", minIntervalMs)
                ));
            }
        } else {
            // 🎯 新版本客户端：只记录日志，不限制频率（信任其自身的指数退避）
            if (lastConnection != null && (now - lastConnection) < 1000) {
                log.info("[SSE RECONNECT] 新版本用户 {} 在{}ms内重连（信任客户端指数退避，允许连接）", 
                        userId, now - lastConnection);
            }
            // 重置旧版本的拒绝计数（如果用户升级了客户端）
            userRejectionCount.put(userId, 0);
        }
        
        // 🚫 限制同一用户的并发连接数（仅限制旧版本，新版本只计数不限制）
        Integer currentCount = userConnectionCount.compute(userId, (k, v) -> (v == null ? 0 : v) + 1);
        
        // 只对旧版本进行并发数限制
        if (!isNewVersionClient && currentCount > MAX_CONNECTIONS_PER_USER_OLD) {
            userConnectionCount.computeIfPresent(userId, (k, v) -> v - 1); // 回退计数
            userRejectionCount.compute(userId, (k, v) -> (v == null ? 1 : v + 1));
            log.warn("[SSE REJECT] 旧版本用户 {} 已达到最大并发连接数限制 ({}/{}), 拒绝新连接", 
                    userId, currentCount - 1, MAX_CONNECTIONS_PER_USER_OLD);
            return Flux.error(new IllegalStateException(
                String.format("已达到最大并发连接数限制(%d)，请关闭其他标签页或刷新页面以获取最新版本。", MAX_CONNECTIONS_PER_USER_OLD)
            ));
        }
        
        // 记录日志：正常的页面刷新或多标签页
        if (lastConnection != null && (now - lastConnection) < 2000) {
            log.info("[SSE NOTICE] 用户 {} 在2秒内重新建立连接（可能是页面刷新），上次连接: {}ms前", 
                    userId, now - lastConnection);
        }
        
        // 记录新连接时间，并重置拒绝计数（连接成功说明客户端行为正常）
        activeConnections.put(userId, now);
        if (!isNewVersionClient) {
            userRejectionCount.put(userId, 0);  // 只重置旧版本的拒绝计数
        }
        
        // 🔧 新版本：只打印日志，不限制
        if (isNewVersionClient) {
            log.info("[SSE CONNECT] 新版本用户 {} 建立SSE连接 [并发: {}, 不限制], 全局活跃连接数: {}", 
                    userId, currentCount, activeConnections.size());
        } else {
            log.info("[SSE CONNECT] 旧版本用户 {} 建立SSE连接 [并发: {}/{}], 全局活跃连接数: {}", 
                    userId, currentCount, MAX_CONNECTIONS_PER_USER_OLD, activeConnections.size());
        }
        
        // 定期清理超过5分钟未活跃的连接记录
        if (activeConnections.size() > 100) {
            cleanupStaleConnections(now);
        }

        // 过滤到当前用户的事件，并设置心跳，防止中间层断开
        // === 生成到期complete信号 ===
        String authHeader = request.getHeaders().getFirst("Authorization");
        java.time.Duration untilExpire = java.time.Duration.ofDays(365);
        Integer tokenVersionInToken = null;
        try {
            if (authHeader != null && authHeader.startsWith("Bearer ")) {
                String token = authHeader.substring(7);
                java.util.Date exp = jwtService.extractExpiration(token);
                tokenVersionInToken = jwtService.extractTokenVersion(token);
                if (exp != null) {
                    java.time.Instant nowInstant = java.time.Instant.now();
                    java.time.Instant expInstant = exp.toInstant();
                    if (expInstant.isAfter(nowInstant)) {
                        untilExpire = java.time.Duration.between(nowInstant, expInstant.minusSeconds(1));
                    } else {
                        untilExpire = java.time.Duration.ZERO;
                    }
                }
            }
        } catch (Throwable ignore) {}

        ServerSentEvent<Map<String, Object>> keepAliveSse = ServerSentEvent.<Map<String, Object>>builder()
                .comment("keepalive")
                .build();
        ServerSentEvent<Map<String, Object>> completeSse = ServerSentEvent.<Map<String, Object>>builder()
                .event("complete")
                .data(java.util.Map.of("data", "[DONE]"))
                .retry(java.time.Duration.ofDays(365))
                .build();

        Flux<ServerSentEvent<Map<String, Object>>> messageFlux = taskEventPublisher.events()
                .filter(ev -> {
                    Object uid = ev.get("userId");
                    // 🔒 严格过滤：只允许匹配当前userId的事件通过
                    boolean pass = (uid != null && userId.equals(String.valueOf(uid)));
                    if (!pass) {
                        log.debug("[SSE FILTER MISS] expectUserId={} actualUserId={}", userId, uid);
                    }
                    return pass;
                })
                .map(ev -> {
                    try {
                        Object type = ev.get("type");
                        Object taskId = ev.get("taskId");
                        Object parent = ev.get("parentTaskId");
                        Object uid = ev.get("userId");
                        boolean hasResult = ev.containsKey("result");
                        log.info("[SSE OUT] type={} taskId={} parentTaskId={} userId={} hasResult={}", type, taskId, parent, uid, hasResult);
                    } catch (Throwable ignore) {}
                    return ServerSentEvent.<Map<String, Object>>builder()
                            .event("message")
                            .data(ev)
                            .build();
                })
                ;

        Flux<ServerSentEvent<Map<String, Object>>> heartbeatFlux = Flux.interval(Duration.ofSeconds(20))
                .map(tick -> {
                    activeConnections.put(userId, System.currentTimeMillis());
                    return keepAliveSse;
                });

        Mono<ServerSentEvent<Map<String, Object>>> completeOnce = Mono.delay(untilExpire).thenReturn(completeSse);

        // 版本变更检测（数据库轮询，单机亦可）：每20秒比对一次用户tokenVersion
        final Integer tokenVersionInTokenFinal = tokenVersionInToken;
        Flux<ServerSentEvent<Map<String, Object>>> versionWatcher = Flux.interval(Duration.ofSeconds(20))
            .flatMap(tick -> com.ainovel.server.config.SpringContextHolder.getBean(com.ainovel.server.service.UserService.class)
                .findUserById(userId)
                .flatMap(user -> {
                    Integer currentVersion = user.getTokenVersion() == null ? 1 : user.getTokenVersion();
                    if (tokenVersionInTokenFinal != null && !currentVersion.equals(tokenVersionInTokenFinal)) {
                        log.info("[SSE TOKEN VERSION CHANGE] 用户 {} token版本变更（token: {}, db: {}），发送complete信号", 
                                userId, tokenVersionInTokenFinal, currentVersion);
                        return Mono.just(completeSse); // 版本号变更：强制complete
                    }
                    return Mono.empty(); // 版本号未变更，不发送事件
                })
            )
            .take(1);

        return Flux.merge(messageFlux, heartbeatFlux)
                .takeUntilOther(Flux.merge(completeOnce, versionWatcher))
                .concatWith(Flux.merge(completeOnce, versionWatcher).take(1))
                .doOnCancel(() -> {
                    // 连接取消时清理记录和计数
                    activeConnections.remove(userId);
                    userConnectionCount.computeIfPresent(userId, (k, v) -> v > 1 ? v - 1 : null);
                    int remaining = userConnectionCount.getOrDefault(userId, 0);
                    log.info("[SSE DISCONNECT] 用户 {} 断开SSE连接 [剩余并发: {}], 全局活跃连接数: {}", 
                            userId, remaining, activeConnections.size());
                })
                .doOnComplete(() -> {
                    // 连接完成时清理记录和计数
                    activeConnections.remove(userId);
                    userConnectionCount.computeIfPresent(userId, (k, v) -> v > 1 ? v - 1 : null);
                    int remaining = userConnectionCount.getOrDefault(userId, 0);
                    log.info("[SSE COMPLETE] 用户 {} SSE连接完成 [剩余并发: {}], 全局活跃连接数: {}", 
                            userId, remaining, activeConnections.size());
                })
                .doOnError(e -> {
                    // 连接错误时清理记录和计数
                    activeConnections.remove(userId);
                    userConnectionCount.computeIfPresent(userId, (k, v) -> v > 1 ? v - 1 : null);
                    int remaining = userConnectionCount.getOrDefault(userId, 0);
                    log.error("[SSE ERROR] 用户 {} SSE连接错误 [剩余并发: {}]: {}", userId, remaining, e.getMessage());
                })
                .onErrorResume(e -> {
                    log.error("SSE 任务事件流错误: {}", e.getMessage(), e);
                    return Flux.empty();
                });
    }
    
    /**
     * 检查客户端版本是否过旧
     * 
     * @param clientVersion 客户端版本（如 "1.5.1"）
     * @param minVersion 最低要求版本（如 "1.5.2"）
     * @return true表示客户端版本过旧
     */
    private boolean isClientVersionTooOld(String clientVersion, String minVersion) {
        try {
            String[] clientParts = clientVersion.split("\\.");
            String[] minParts = minVersion.split("\\.");
            
            // 比较major版本
            int clientMajor = Integer.parseInt(clientParts[0]);
            int minMajor = Integer.parseInt(minParts[0]);
            if (clientMajor < minMajor) return true;
            if (clientMajor > minMajor) return false;
            
            // major相同，比较minor版本
            if (clientParts.length > 1 && minParts.length > 1) {
                int clientMinor = Integer.parseInt(clientParts[1]);
                int minMinor = Integer.parseInt(minParts[1]);
                if (clientMinor < minMinor) return true;
                if (clientMinor > minMinor) return false;
            }
            
            // major和minor相同，比较patch版本
            if (clientParts.length > 2 && minParts.length > 2) {
                int clientPatch = Integer.parseInt(clientParts[2]);
                int minPatch = Integer.parseInt(minParts[2]);
                return clientPatch < minPatch;
            }
            
            return false;
        } catch (Exception e) {
            log.warn("解析版本号失败: clientVersion={}, minVersion={}, error={}", 
                    clientVersion, minVersion, e.getMessage());
            // 解析失败时保守处理，认为版本过旧
            return true;
        }
    }
    
    /**
     * 清理超过5分钟未活跃的连接记录
     */
    private void cleanupStaleConnections(long now) {
        // 清理过期的连接记录
        activeConnections.entrySet().removeIf(entry -> 
            (now - entry.getValue()) > Duration.ofMinutes(5).toMillis()
        );
        
        // 清理没有活跃连接的用户计数记录
        userConnectionCount.entrySet().removeIf(entry -> 
            !activeConnections.containsKey(entry.getKey()) && entry.getValue() == 0
        );
        
        // 清理过期的拒绝计数记录（超过5分钟）
        userRejectionResetTime.entrySet().removeIf(entry -> 
            (now - entry.getValue()) > Duration.ofMinutes(5).toMillis()
        );
        userRejectionCount.entrySet().removeIf(entry -> 
            !userRejectionResetTime.containsKey(entry.getKey())
        );
        
        log.debug("[SSE CLEANUP] 清理过期连接记录，剩余活跃连接数: {}, 剩余用户计数: {}, 剩余拒绝计数: {}", 
                activeConnections.size(), userConnectionCount.size(), userRejectionCount.size());
    }
    // 事件发布从控制器内嵌迁移至独立 TaskEventPublisherImpl
}


