package com.ainovel.server.web.controller;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Comparator;

import reactor.core.publisher.Flux;

import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import com.ainovel.server.common.response.ApiResponse;
import com.ainovel.server.security.CurrentUser;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import com.ainovel.server.domain.model.analytics.WritingEvent;
import com.ainovel.server.domain.model.observability.LLMTrace;
import com.ainovel.server.repository.NovelRepository;
import com.ainovel.server.service.ai.observability.LLMTraceService;
import com.ainovel.server.service.analytics.WritingAnalyticsService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/analytics")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Analytics", description = "数据分析统计接口")
public class AnalyticsController {

    private final LLMTraceService llmTraceService;
    private final NovelRepository novelRepository;
    private final WritingAnalyticsService writingAnalyticsService;

    @GetMapping("/overview")
    @Operation(summary = "获取分析概览数据", description = "获取用户的写作统计概览")
    public Mono<ApiResponse<Map<String, Object>>> getAnalyticsOverview(@AuthenticationPrincipal CurrentUser currentUser) {
        log.info("获取用户 {} 的分析概览数据", currentUser.getId());
        String userId = currentUser.getId();
        
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime monthStart = now.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        
        return Mono.zip(
                // 总字数统计
                novelRepository.findByAuthorId(userId)
                    .filter(novel -> novel.getMetadata() != null)
                    .map(novel -> novel.getMetadata().getWordCount())
                    .reduce(0, Integer::sum),
                
                // 本月新增字数（基于小说updatedAt在本月内变更过，取当前字数作为新增的近似值）
                novelRepository.findByAuthorId(userId)
                    .filter(novel -> novel.getUpdatedAt() != null && !novel.getUpdatedAt().isBefore(monthStart))
                    .filter(novel -> novel.getMetadata() != null)
                    .map(novel -> novel.getMetadata().getWordCount())
                    .reduce(0, Integer::sum),
                
                // Token统计（累计）
                llmTraceService.findTracesByUserId(userId, org.springframework.data.domain.PageRequest.of(0, 10000))
                    .filter(trace -> trace.getResponse() != null && 
                            trace.getResponse().getMetadata() != null && 
                            trace.getResponse().getMetadata().getTokenUsage() != null)
                    .map(trace -> {
                        Integer total = trace.getResponse().getMetadata().getTokenUsage().getTotalTokenCount();
                        return total != null ? total : 0;
                    })
                    .reduce(0, Integer::sum),
                
                // 本月Token统计
                llmTraceService.findTracesByUserId(userId, org.springframework.data.domain.PageRequest.of(0, 10000))
                    .filter(trace -> !trace.getCreatedAt().isBefore(monthStart.atZone(java.time.ZoneId.systemDefault()).toInstant()))
                    .filter(trace -> trace.getResponse() != null && 
                            trace.getResponse().getMetadata() != null && 
                            trace.getResponse().getMetadata().getTokenUsage() != null)
                    .map(trace -> {
                        Integer total = trace.getResponse().getMetadata().getTokenUsage().getTotalTokenCount();
                        return total != null ? total : 0;
                    })
                    .reduce(0, Integer::sum),
                
                // 功能使用次数（今日之外的全部调用次数）
                llmTraceService.findTracesByUserId(userId, org.springframework.data.domain.PageRequest.of(0, 10000))
                    .count(),
                
                // 最受欢迎功能
                llmTraceService.findTracesByUserId(userId, org.springframework.data.domain.PageRequest.of(0, 10000))
                    .filter(trace -> trace.getBusinessType() != null)
                    .map(LLMTrace::getBusinessType)
                    .collectList()
                    .map(businessTypes -> businessTypes.stream()
                            .collect(Collectors.groupingBy(type -> type, Collectors.counting()))
                            .entrySet().stream()
                            .max(Map.Entry.comparingByValue())
                            .map(entry -> getBusinessTypeName(entry.getKey()))
                            .orElse("智能续写")),
                
                // 写作天数（改为根据写作事件统计）
                writingAnalyticsService.countUniqueWritingDays(userId),
                
                // 连续写作天数（改为根据写作事件统计）
                writingAnalyticsService.calculateConsecutiveWritingDays(userId)
                
        ).map(tuple -> {
            Map<String, Object> overview = new HashMap<>();
            overview.put("totalWords", tuple.getT1());
            overview.put("monthlyNewWords", tuple.getT2());
            overview.put("totalTokens", tuple.getT3());
            overview.put("monthlyNewTokens", tuple.getT4());
            overview.put("functionUsageCount", tuple.getT5());
            overview.put("mostPopularFunction", tuple.getT6());
            overview.put("writingDays", tuple.getT7());
            overview.put("consecutiveDays", tuple.getT8());
            return overview;
        }).map(ApiResponse::success);
    }

    @GetMapping("/token-usage-trend")
    @Operation(summary = "获取Token使用趋势", description = "按时间维度获取Token使用趋势")
    public Mono<ApiResponse<List<Map<String, Object>>>> getTokenUsageTrend(
            @AuthenticationPrincipal CurrentUser currentUser,
            @RequestParam(defaultValue = "monthly") String viewMode,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startDate,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endDate) {
        
        log.info("获取用户 {} 的Token使用趋势, 视图模式: {}", currentUser.getId(), viewMode);
        String userId = currentUser.getId();
        
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime start, end;
        
        switch (viewMode.toLowerCase()) {
            case "daily":
                start = now.minusDays(30);
                end = now;
                break;
            case "monthly":
                start = now.minusMonths(12);
                end = now;
                break;
            case "cumulative":
                start = now.minusMonths(6);
                end = now;
                break;
            case "range":
                start = startDate != null ? startDate : now.minusDays(30);
                end = endDate != null ? endDate : now;
                break;
            default:
                start = now.minusMonths(12);
                end = now;
        }
        
        return llmTraceService.findTracesByUserId(userId, org.springframework.data.domain.PageRequest.of(0, 10000))
                .filter(trace -> trace.getCreatedAt().isAfter(start.atZone(java.time.ZoneId.systemDefault()).toInstant()) &&
                        trace.getCreatedAt().isBefore(end.atZone(java.time.ZoneId.systemDefault()).toInstant()))
                .filter(trace -> !isSettingGenerationCall(trace.getBusinessType())) // 过滤设定生成的工具调用
                .filter(trace -> trace.getResponse() != null && 
                        trace.getResponse().getMetadata() != null && 
                        trace.getResponse().getMetadata().getTokenUsage() != null)
                .collectList()
                .map(traces -> {
                    Map<String, Map<String, Integer>> groupedData = new HashMap<>();
                    DateTimeFormatter formatter = getDateFormatter(viewMode);
                    
                    for (LLMTrace trace : traces) {
                        LocalDate date = trace.getCreatedAt().atZone(java.time.ZoneId.systemDefault()).toLocalDate();
                        String key = date.format(formatter);
                        
                        groupedData.computeIfAbsent(key, k -> {
                            Map<String, Integer> dayData = new HashMap<>();
                            dayData.put("inputTokens", 0);
                            dayData.put("outputTokens", 0);
                            return dayData;
                        });
                        
                        var tokenUsage = trace.getResponse().getMetadata().getTokenUsage();
                        groupedData.get(key).merge("inputTokens", 
                            tokenUsage.getInputTokenCount() != null ? tokenUsage.getInputTokenCount() : 0, 
                            Integer::sum);
                        groupedData.get(key).merge("outputTokens", 
                            tokenUsage.getOutputTokenCount() != null ? tokenUsage.getOutputTokenCount() : 0, 
                            Integer::sum);
                    }
                    
                    List<Map<String, Object>> result = new ArrayList<>();
                    List<String> sortedKeys = new ArrayList<>(groupedData.keySet());
                    Collections.sort(sortedKeys);
                    
                    int cumulativeInput = 0;
                    int cumulativeOutput = 0;
                    
                    for (String key : sortedKeys) {
                        Map<String, Integer> dayData = groupedData.get(key);
                        Map<String, Object> item = new HashMap<>();
                        
                        item.put("date", key);
                        if ("cumulative".equals(viewMode)) {
                            cumulativeInput += dayData.get("inputTokens");
                            cumulativeOutput += dayData.get("outputTokens");
                            item.put("inputTokens", cumulativeInput);
                            item.put("outputTokens", cumulativeOutput);
                        } else {
                            item.put("inputTokens", dayData.get("inputTokens"));
                            item.put("outputTokens", dayData.get("outputTokens"));
                        }
                        result.add(item);
                    }
                    
                    return result;
                })
                .map(ApiResponse::success);
    }

    @GetMapping("/function-usage-stats")
    @Operation(summary = "获取功能使用统计", description = "获取各功能的使用情况统计")
    public Mono<ApiResponse<List<Map<String, Object>>>> getFunctionUsageStats(
            @AuthenticationPrincipal CurrentUser currentUser,
            @RequestParam(defaultValue = "daily") String viewMode) {
        
        log.info("获取用户 {} 的功能使用统计", currentUser.getId());
        String userId = currentUser.getId();
        
        return llmTraceService.findTracesByUserId(userId, org.springframework.data.domain.PageRequest.of(0, 1000))
                .filter(trace -> trace.getBusinessType() != null)
                .filter(trace -> !isSettingGenerationCall(trace.getBusinessType())) // 过滤设定生成的工具调用
                .collectList()
                .map(traces -> {
                    Map<String, Long> functionCounts = traces.stream()
                            .collect(Collectors.groupingBy(LLMTrace::getBusinessType, Collectors.counting()));
                    
                    return functionCounts.entrySet().stream()
                            .map(entry -> {
                                Map<String, Object> item = new HashMap<>();
                                item.put("function", getBusinessTypeName(entry.getKey()));
                                item.put("count", entry.getValue());
                                return item;
                            })
                            .sorted((a, b) -> Long.compare((Long) b.get("count"), (Long) a.get("count")))
                            .collect(Collectors.toList());
                })
                .map(ApiResponse::success);
    }

    @GetMapping("/model-usage-stats")
    @Operation(summary = "获取模型使用统计", description = "获取各模型的使用占比统计")
    public Mono<ApiResponse<List<Map<String, Object>>>> getModelUsageStats(
            @AuthenticationPrincipal CurrentUser currentUser,
            @RequestParam(defaultValue = "daily") String viewMode) {
        
        log.info("获取用户 {} 的模型使用统计", currentUser.getId());
        String userId = currentUser.getId();
        
        return llmTraceService.findTracesByUserId(userId, org.springframework.data.domain.PageRequest.of(0, 1000))
                .filter(trace -> trace.getModel() != null)
                .filter(trace -> !isSettingGenerationCall(trace.getBusinessType())) // 过滤设定生成的工具调用
                .collectList()
                .map(traces -> {
                    Map<String, Long> modelCounts = traces.stream()
                            .collect(Collectors.groupingBy(LLMTrace::getModel, Collectors.counting()));
                    
                    long total = modelCounts.values().stream().mapToLong(Long::longValue).sum();
                    
                    return modelCounts.entrySet().stream()
                            .map(entry -> {
                                Map<String, Object> item = new HashMap<>();
                                item.put("model", entry.getKey());
                                item.put("count", entry.getValue());
                                item.put("percentage", total > 0 ? (double) entry.getValue() / total * 100 : 0);
                                return item;
                            })
                            .sorted((a, b) -> Long.compare((Long) b.get("count"), (Long) a.get("count")))
                            .collect(Collectors.toList());
                })
                .map(ApiResponse::success);
    }

    @GetMapping("/token-usage-records")
    @Operation(summary = "获取Token使用记录", description = "获取最近的Token使用记录")
    public Mono<ApiResponse<List<Map<String, Object>>>> getTokenUsageRecords(
            @AuthenticationPrincipal CurrentUser currentUser,
            @RequestParam(defaultValue = "8") int limit) {
        
        log.info("获取用户 {} 的Token使用记录，限制 {} 条", currentUser.getId(), limit);
        String userId = currentUser.getId();
        
        return llmTraceService.findTracesByUserId(userId, org.springframework.data.domain.PageRequest.of(0, limit))
                .filter(trace -> !isSettingGenerationCall(trace.getBusinessType())) // 过滤设定生成的工具调用
                .map(trace -> {
                    Map<String, Object> record = new HashMap<>();
                    var tokenUsage = (trace.getResponse() != null && trace.getResponse().getMetadata() != null)
                            ? trace.getResponse().getMetadata().getTokenUsage()
                            : null;

                    record.put("id", trace.getId() != null ? trace.getId() : trace.getTraceId());
                    record.put("model", trace.getModel() != null ? trace.getModel() : "Unknown");
                    record.put("taskType", getBusinessTypeName(trace.getBusinessType()));
                    record.put("inputTokens", tokenUsage != null && tokenUsage.getInputTokenCount() != null ? tokenUsage.getInputTokenCount() : 0);
                    record.put("outputTokens", tokenUsage != null && tokenUsage.getOutputTokenCount() != null ? tokenUsage.getOutputTokenCount() : 0);
                    record.put("cost", calculateCost(
                            tokenUsage != null ? tokenUsage.getInputTokenCount() : null,
                            tokenUsage != null ? tokenUsage.getOutputTokenCount() : null));
                    record.put("timestamp", trace.getCreatedAt());
                    
                    return record;
                })
                .collectList()
                .map(ApiResponse::success);
    }

    @GetMapping("/today-summary")
    @Operation(summary = "获取今日Token使用汇总", description = "获取今日的Token使用汇总统计")
    public Mono<ApiResponse<Map<String, Object>>> getTodayTokenSummary(@AuthenticationPrincipal CurrentUser currentUser) {
        log.info("获取用户 {} 的今日Token使用汇总", currentUser.getId());
        String userId = currentUser.getId();
        
        LocalDate today = LocalDate.now();
        var startOfDay = today.atStartOfDay().atZone(java.time.ZoneId.systemDefault()).toInstant();
        var endOfDay = today.plusDays(1).atStartOfDay().atZone(java.time.ZoneId.systemDefault()).toInstant();

        return llmTraceService.findTracesByUserId(userId, org.springframework.data.domain.PageRequest.of(0, 10000))
                .filter(trace -> !trace.getCreatedAt().isBefore(startOfDay) && trace.getCreatedAt().isBefore(endOfDay))
                .filter(trace -> !isSettingGenerationCall(trace.getBusinessType())) // 过滤设定生成的工具调用
                .collectList()
                .map(traces -> {
                    Map<String, Object> summary = new HashMap<>();

                    int totalRecords = traces.size();
                    int totalTokens = traces.stream()
                            .filter(t -> t.getResponse() != null && t.getResponse().getMetadata() != null && t.getResponse().getMetadata().getTokenUsage() != null)
                            .mapToInt(t -> {
                                var u = t.getResponse().getMetadata().getTokenUsage();
                                int inTok = u.getInputTokenCount() != null ? u.getInputTokenCount() : 0;
                                int outTok = u.getOutputTokenCount() != null ? u.getOutputTokenCount() : 0;
                                return inTok + outTok;
                            })
                            .sum();

                    double totalCost = traces.stream()
                            .filter(t -> t.getResponse() != null && t.getResponse().getMetadata() != null && t.getResponse().getMetadata().getTokenUsage() != null)
                            .mapToDouble(t -> {
                                var u = t.getResponse().getMetadata().getTokenUsage();
                                return calculateCost(u.getInputTokenCount(), u.getOutputTokenCount());
                            })
                            .sum();

                    summary.put("totalRecords", totalRecords);
                    summary.put("totalTokens", totalTokens);
                    summary.put("totalCost", totalCost);

                    return summary;
                })
                .map(ApiResponse::success);
    }

    // 辅助方法
    private DateTimeFormatter getDateFormatter(String viewMode) {
        switch (viewMode.toLowerCase()) {
            case "daily":
                return DateTimeFormatter.ofPattern("MM-dd");
            case "monthly":
                return DateTimeFormatter.ofPattern("yyyy-MM");
            case "cumulative":
            case "range":
                return DateTimeFormatter.ofPattern("MM-dd");
            default:
                return DateTimeFormatter.ofPattern("yyyy-MM");
        }
    }

    private String getBusinessTypeName(String businessType) {
        if (businessType == null) return "未知功能";
        
        switch (businessType.toUpperCase()) {
            case "SCENE_TO_SUMMARY":
                return "场景摘要";
            case "SUMMARY_TO_SCENE":
                return "摘要扩写";
            case "TEXT_EXPANSION":
                return "文本扩写";
            case "TEXT_REFACTOR":
                return "文本重构";
            case "TEXT_SUMMARY":
                return "文本总结";
            case "AI_CHAT":
                return "AI聊天";
            case "NOVEL_GENERATION":
                return "小说生成";
            case "PROFESSIONAL_FICTION_CONTINUATION":
                return "专业续写";
            case "SCENE_BEAT_GENERATION":
                return "场景节拍生成";
            case "NOVEL_COMPOSE":
                return "设定编排";
            case "SETTING_TREE_GENERATION":
                return "设定树生成";
            // 设定生成相关的业务类型
            case "SETTING_TEXT_STREAM":
                return "设定生成";
            case "SETTING_TOOL_STAGE_INC":
                return "设定生成";
            case "SETTING_GENERATION":
                return "设定生成";
            // 兼容旧版本的映射
            case "CONTENT_OPTIMIZATION":
                return "内容优化";
            case "GRAMMAR_CHECK":
                return "语法检查";
            case "STYLE_IMPROVEMENT":
                return "风格改进";
            default:
                return businessType;
        }
    }

    private double calculateCost(Integer inputTokens, Integer outputTokens) {
        // 简单的成本计算，实际应该根据不同模型定价
        double inputCost = (inputTokens != null ? inputTokens : 0) * 0.0001; // 每千token 0.1美元
        double outputCost = (outputTokens != null ? outputTokens : 0) * 0.0002; // 输出更贵
        return inputCost + outputCost;
    }

    /**
     * 判断是否为设定生成的内部工具调用（需要过滤的调用）
     * SETTING_TEXT_STREAM 是文本阶段，需要保留；只过滤内部工具调用
     */
    private boolean isSettingGenerationCall(String businessType) {
        if (businessType == null) return false;
        String type = businessType.toUpperCase();
        // 只过滤内部工具调用，保留文本阶段
        return type.equals("SETTING_TOOL_STAGE_INC") || 
               (type.startsWith("SETTING_") && (type.contains("TOOL") || type.contains("STAGE")) && !type.equals("SETTING_TEXT_STREAM"));
    }

    // 保留此方法以备将来可能的连续写作天数统计功能使用
    @SuppressWarnings("unused")
    private Mono<Long> calculateConsecutiveDays(String userId) {
        return novelRepository.findByAuthorId(userId)
                .filter(novel -> novel.getUpdatedAt() != null)
                .map(novel -> novel.getUpdatedAt().toLocalDate())
                .distinct()
                .sort(Comparator.reverseOrder())
                .collectList()
                .map(dates -> {
                    if (dates.isEmpty()) return 0L;
                    
                    long consecutiveDays = 1;
                    LocalDate previousDate = dates.get(0);
                    
                    for (int i = 1; i < dates.size(); i++) {
                        LocalDate currentDate = dates.get(i);
                        if (previousDate.minusDays(1).equals(currentDate)) {
                            consecutiveDays++;
                            previousDate = currentDate;
                        } else {
                            break;
                        }
                    }
                    
                    return consecutiveDays;
                });
    }

    // ==================== 整合来自 UserLLMAnalyticsController 的接口 ====================
    
    @GetMapping("/llm/daily-tokens")
    @Operation(summary = "Token日统计", description = "按天统计当前用户的Token消耗")
    public Mono<ApiResponse<Map<String, Integer>>> getDailyTokens(
        @AuthenticationPrincipal CurrentUser currentUser,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startTime,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endTime
    ) {
        log.info("获取用户 {} 的Token日统计", currentUser.getId());
        String userId = currentUser.getId();
        return llmTraceService.getUserDailyTokens(userId, startTime, endTime).map(ApiResponse::success);
    }

    @GetMapping("/llm/features")
    @Operation(summary = "功能使用统计", description = "按业务功能聚合调用次数与Token")
    public Mono<ApiResponse<Map<String, Object>>> getFeatureStats(
        @AuthenticationPrincipal CurrentUser currentUser,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startTime,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endTime
    ) {
        log.info("获取用户 {} 的功能使用统计", currentUser.getId());
        String userId = currentUser.getId();
        return llmTraceService.getUserFeatureStatistics(userId, startTime, endTime).map(ApiResponse::success);
    }

    // ==================== 整合来自 WritingAnalyticsController 的接口 ====================

    @GetMapping("/writing/events")
    @Operation(summary = "写作事件记录", description = "按时间倒序返回最近写作事件")
    public Flux<WritingEvent> getWritingEvents(
        @AuthenticationPrincipal CurrentUser currentUser,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "50") int size
    ) {
        log.info("获取用户 {} 的写作事件记录", currentUser.getId());
        String userId = currentUser.getId();
        return writingAnalyticsService.listUserEvents(userId, page, size);
    }

    @GetMapping("/writing/daily")
    @Operation(summary = "每日字数统计", description = "区间每日净新增字数")
    public Mono<ApiResponse<Map<String, Object>>> getDailyWritingStats(
        @AuthenticationPrincipal CurrentUser currentUser,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
        @RequestParam(required = false) String novelId,
        @RequestParam(required = false) String chapterId,
        @RequestParam(required = false) String sceneId
    ) {
        log.info("获取用户 {} 的每日字数统计", currentUser.getId());
        String userId = currentUser.getId();
        return writingAnalyticsService.aggregateUserDaily(userId, start, end, novelId, chapterId, sceneId)
                .map(ApiResponse::success);
    }

    @GetMapping("/writing/source")
    @Operation(summary = "写作来源统计", description = "手动/AI来源的字数占比")
    public Mono<ApiResponse<Map<String, Object>>> getWritingSourceStats(
        @AuthenticationPrincipal CurrentUser currentUser,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
        @RequestParam(required = false) String novelId,
        @RequestParam(required = false) String chapterId,
        @RequestParam(required = false) String sceneId
    ) {
        log.info("获取用户 {} 的写作来源统计", currentUser.getId());
        String userId = currentUser.getId();
        return writingAnalyticsService.aggregateBySource(userId, start, end, novelId, chapterId, sceneId)
                .map(ApiResponse::success);
    }
}
