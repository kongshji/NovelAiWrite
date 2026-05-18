package com.ainovel.server.web.controller;

import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ainovel.server.common.response.ApiResponse;
import com.ainovel.server.service.impl.content.ContentProviderFactory;
import com.ainovel.server.service.prompt.PlaceholderDescriptionService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;

/**
 * 内容提供器状态API控制器
 * 提供内容提供器实现状态和占位符可用性查询
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/content-provider")
@Tag(name = "内容提供器", description = "内容提供器实现状态和占位符管理")
public class ContentProviderController {

    @Autowired
    private ContentProviderFactory contentProviderFactory;
    
    @Autowired
    private PlaceholderDescriptionService placeholderDescriptionService;

    /**
     * 获取所有已实现的内容提供器类型
     */
    @GetMapping("/available-types")
    @Operation(summary = "获取可用的内容提供器类型", description = "返回所有已注册和实现的内容提供器类型")
    public ApiResponse<Set<String>> getAvailableContentProviderTypes() {
        Set<String> availableTypes = contentProviderFactory.getAvailableTypes();
        log.info("返回可用内容提供器类型: {}", availableTypes);
        return ApiResponse.success(availableTypes);
    }

    /**
     * 检查指定内容提供器是否已实现
     */
    @GetMapping("/check-providers")
    @Operation(summary = "批量检查内容提供器状态", description = "检查指定的内容提供器类型是否已实现")
    public ApiResponse<Map<String, Boolean>> checkContentProviders(@RequestParam Set<String> types) {
        Map<String, Boolean> providerStatus = contentProviderFactory.checkProviders(types);
        log.info("内容提供器状态检查: 请求={}, 结果={}", types, providerStatus);
        return ApiResponse.success(providerStatus);
    }

    /**
     * 获取所有可用的占位符
     */
    @GetMapping("/available-placeholders")
    @Operation(summary = "获取可用占位符", description = "返回所有实际可用的占位符（已过滤掉未实现的内容提供器）")
    public ApiResponse<Set<String>> getAvailablePlaceholders() {
        Set<String> availablePlaceholders = placeholderDescriptionService.getAvailablePlaceholders();
        log.info("返回可用占位符数量: {}", availablePlaceholders.size());
        return ApiResponse.success(availablePlaceholders);
    }

    /**
     * 获取占位符描述映射
     */
    @GetMapping("/placeholder-descriptions")
    @Operation(summary = "获取占位符描述", description = "获取指定占位符的详细描述信息")
    public ApiResponse<Map<String, String>> getPlaceholderDescriptions(@RequestParam Set<String> placeholders) {
        Map<String, String> descriptions = placeholderDescriptionService.getPlaceholderDescriptions(placeholders);
        log.info("返回占位符描述: 请求={}, 描述数量={}", placeholders.size(), descriptions.size());
        return ApiResponse.success(descriptions);
    }

    /**
     * 过滤占位符，只返回可用的
     */
    @GetMapping("/filter-placeholders")
    @Operation(summary = "过滤可用占位符", description = "从请求的占位符中过滤出实际可用的占位符")
    public ApiResponse<FilterResult> filterAvailablePlaceholders(@RequestParam Set<String> requestedPlaceholders) {
        Set<String> availablePlaceholders = placeholderDescriptionService.filterAvailablePlaceholders(requestedPlaceholders);
        Set<String> unavailablePlaceholders = new java.util.HashSet<>(requestedPlaceholders);
        unavailablePlaceholders.removeAll(availablePlaceholders);
        
        FilterResult result = new FilterResult(
                requestedPlaceholders,
                availablePlaceholders, 
                unavailablePlaceholders,
                placeholderDescriptionService.getPlaceholderDescriptions(availablePlaceholders)
        );
        
        log.info("占位符过滤结果: 请求={}, 可用={}, 不可用={}", 
                requestedPlaceholders.size(), availablePlaceholders.size(), unavailablePlaceholders.size());
        
        return ApiResponse.success(result);
    }

    /**
     * 获取完整的内容提供器和占位符状态报告
     */
    @GetMapping("/status-report")
    @Operation(summary = "获取状态报告", description = "获取内容提供器和占位符的完整状态报告")
    public ApiResponse<StatusReport> getStatusReport() {
        Set<String> availableProviders = contentProviderFactory.getAvailableTypes();
        Set<String> availablePlaceholders = placeholderDescriptionService.getAvailablePlaceholders();
        Map<String, String> placeholderDescriptions = placeholderDescriptionService.getPlaceholderDescriptions(availablePlaceholders);
        
        StatusReport report = new StatusReport(
                availableProviders,
                availablePlaceholders,
                placeholderDescriptions,
                availableProviders.size(),
                availablePlaceholders.size()
        );
        
        log.info("生成状态报告: 提供器数={}, 占位符数={}", 
                availableProviders.size(), availablePlaceholders.size());
        
        return ApiResponse.success(report);
    }

    // ==================== 数据传输对象 ====================

    /**
     * 过滤结果
     */
    public static class FilterResult {
        private final Set<String> requestedPlaceholders;
        private final Set<String> availablePlaceholders;
        private final Set<String> unavailablePlaceholders;
        private final Map<String, String> descriptions;

        public FilterResult(Set<String> requestedPlaceholders, Set<String> availablePlaceholders,
                          Set<String> unavailablePlaceholders, Map<String, String> descriptions) {
            this.requestedPlaceholders = requestedPlaceholders;
            this.availablePlaceholders = availablePlaceholders;
            this.unavailablePlaceholders = unavailablePlaceholders;
            this.descriptions = descriptions;
        }

        public Set<String> getRequestedPlaceholders() { return requestedPlaceholders; }
        public Set<String> getAvailablePlaceholders() { return availablePlaceholders; }
        public Set<String> getUnavailablePlaceholders() { return unavailablePlaceholders; }
        public Map<String, String> getDescriptions() { return descriptions; }
    }

    /**
     * 状态报告
     */
    public static class StatusReport {
        private final Set<String> availableProviders;
        private final Set<String> availablePlaceholders;
        private final Map<String, String> placeholderDescriptions;
        private final int providerCount;
        private final int placeholderCount;

        public StatusReport(Set<String> availableProviders, Set<String> availablePlaceholders,
                          Map<String, String> placeholderDescriptions, int providerCount, int placeholderCount) {
            this.availableProviders = availableProviders;
            this.availablePlaceholders = availablePlaceholders;
            this.placeholderDescriptions = placeholderDescriptions;
            this.providerCount = providerCount;
            this.placeholderCount = placeholderCount;
        }

        public Set<String> getAvailableProviders() { return availableProviders; }
        public Set<String> getAvailablePlaceholders() { return availablePlaceholders; }
        public Map<String, String> getPlaceholderDescriptions() { return placeholderDescriptions; }
        public int getProviderCount() { return providerCount; }
        public int getPlaceholderCount() { return placeholderCount; }
    }
} 