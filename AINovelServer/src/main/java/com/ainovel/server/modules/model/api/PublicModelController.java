package com.ainovel.server.web.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ainovel.server.common.response.ApiResponse;
import com.ainovel.server.service.PublicModelConfigService;
import com.ainovel.server.web.dto.response.PublicModelResponseDto;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 公共模型控制器
 * 提供前端安全访问公共模型列表的API端点
 */
@Slf4j
@RestController
@RequestMapping("/api/v1")
@Tag(name = "PublicModel", description = "公共模型API")
public class PublicModelController {

    @Autowired
    private PublicModelConfigService publicModelConfigService;

    /**
     * 获取公共模型列表
     * 只包含向前端暴露的安全信息，不含API Keys等敏感数据
     * 用户必须登录才能访问此接口
     * 
     * @return 公共模型响应DTO列表
     */
    @GetMapping("/public-models")
    @Operation(summary = "获取公共模型列表", description = "获取所有启用的公共模型，按优先级排序。不包含敏感信息如API Keys。")
    @PreAuthorize("hasRole('USER') or hasRole('ADMIN')")
    public Mono<ResponseEntity<ApiResponse<List<PublicModelResponseDto>>>> getPublicModels() {
        log.info("获取公共模型列表请求");
        
        return publicModelConfigService.getPublicModels()
                .collectList()
                .map(models -> {
                    log.info("返回 {} 个公共模型", models.size());
                    return ResponseEntity.ok(ApiResponse.success(models));
                })
                .doOnError(error -> log.error("获取公共模型列表失败", error))
                .onErrorResume(error -> {
                    log.error("获取公共模型列表时发生错误: {}", error.getMessage());
                    return Mono.just(ResponseEntity.internalServerError()
                            .body(ApiResponse.error("获取公共模型列表失败: " + error.getMessage())));
                });
    }
} 