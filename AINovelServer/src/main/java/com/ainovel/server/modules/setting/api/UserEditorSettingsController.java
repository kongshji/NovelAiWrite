package com.ainovel.server.web.controller;

import com.ainovel.server.domain.model.UserEditorSettings;
import com.ainovel.server.service.UserEditorSettingsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

/**
 * 用户编辑器设置控制器
 */
@RestController
@RequestMapping({"/api/v1/user-editor-settings", "/api/v1/api/user-editor-settings"})
public class UserEditorSettingsController {
    
    private static final Logger logger = LoggerFactory.getLogger(UserEditorSettingsController.class);
    
    @Autowired
    private UserEditorSettingsService userEditorSettingsService;
    
    /**
     * 获取用户编辑器设置
     * @param userId 用户ID
     * @return 用户编辑器设置
     */
    @GetMapping("/{userId}")
    public Mono<ResponseEntity<UserEditorSettings>> getUserEditorSettings(@PathVariable String userId) {
        logger.info("获取用户编辑器设置请求: userId={}", userId);
        
        return userEditorSettingsService.getUserEditorSettings(userId)
                .map(settings -> {
                    logger.info("成功获取用户编辑器设置: userId={}, settingsId={}", userId, settings.getId());
                    return ResponseEntity.ok(settings);
                })
                .onErrorResume(error -> {
                    logger.error("获取用户编辑器设置失败: userId={}, error={}", userId, error.getMessage());
                    return Mono.just(ResponseEntity.internalServerError().build());
                });
    }
    
    /**
     * 保存/更新用户编辑器设置
     * @param userId 用户ID
     * @param settings 编辑器设置
     * @return 保存后的设置
     */
    @PostMapping("/{userId}")
    public Mono<ResponseEntity<UserEditorSettings>> saveUserEditorSettings(
            @PathVariable String userId,
            @RequestBody UserEditorSettings settings) {
        
        logger.info("保存用户编辑器设置请求: userId={}", userId);
        
        // 确保用户ID一致
        settings.setUserId(userId);
        
        return userEditorSettingsService.updateUserEditorSettings(userId, settings)
                .map(savedSettings -> {
                    logger.info("成功保存用户编辑器设置: userId={}, settingsId={}", userId, savedSettings.getId());
                    return ResponseEntity.ok(savedSettings);
                })
                .onErrorResume(error -> {
                    logger.error("保存用户编辑器设置失败: userId={}, error={}", userId, error.getMessage());
                    return Mono.just(ResponseEntity.internalServerError().build());
                });
    }
    
    /**
     * 部分更新用户编辑器设置
     * @param userId 用户ID
     * @param settings 要更新的设置字段
     * @return 更新后的设置
     */
    @PatchMapping("/{userId}")
    public Mono<ResponseEntity<UserEditorSettings>> updateUserEditorSettings(
            @PathVariable String userId,
            @RequestBody UserEditorSettings settings) {
        
        logger.info("更新用户编辑器设置请求: userId={}", userId);
        
        // 确保用户ID一致
        settings.setUserId(userId);
        
        return userEditorSettingsService.updateUserEditorSettings(userId, settings)
                .map(updatedSettings -> {
                    logger.info("成功更新用户编辑器设置: userId={}, settingsId={}", userId, updatedSettings.getId());
                    return ResponseEntity.ok(updatedSettings);
                })
                .onErrorResume(error -> {
                    logger.error("更新用户编辑器设置失败: userId={}, error={}", userId, error.getMessage());
                    return Mono.just(ResponseEntity.internalServerError().build());
                });
    }
    
    /**
     * 删除用户编辑器设置（重置为默认）
     * @param userId 用户ID
     * @return 删除结果
     */
    @DeleteMapping("/{userId}")
    public Mono<ResponseEntity<Void>> deleteUserEditorSettings(@PathVariable String userId) {
        logger.info("删除用户编辑器设置请求: userId={}", userId);
        
        return userEditorSettingsService.deleteUserEditorSettings(userId)
                .then(Mono.fromCallable(() -> {
                    logger.info("成功删除用户编辑器设置: userId={}", userId);
                    return ResponseEntity.ok().<Void>build();
                }))
                .onErrorResume(error -> {
                    logger.error("删除用户编辑器设置失败: userId={}, error={}", userId, error.getMessage());
                    return Mono.just(ResponseEntity.internalServerError().build());
                });
    }
    
    /**
     * 重置用户编辑器设置为默认值
     * @param userId 用户ID
     * @return 重置后的默认设置
     */
    @PostMapping("/{userId}/reset")
    public Mono<ResponseEntity<UserEditorSettings>> resetUserEditorSettings(@PathVariable String userId) {
        logger.info("重置用户编辑器设置请求: userId={}", userId);
        
        return userEditorSettingsService.deleteUserEditorSettings(userId)
                .then(userEditorSettingsService.getUserEditorSettings(userId))
                .map(defaultSettings -> {
                    logger.info("成功重置用户编辑器设置: userId={}, settingsId={}", userId, defaultSettings.getId());
                    return ResponseEntity.ok(defaultSettings);
                })
                .onErrorResume(error -> {
                    logger.error("重置用户编辑器设置失败: userId={}, error={}", userId, error.getMessage());
                    return Mono.just(ResponseEntity.internalServerError().build());
                });
    }
} 