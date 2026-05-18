package com.ainovel.server.web.controller;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.ainovel.server.common.response.ApiResponse;
import com.ainovel.server.security.CurrentUser;
import com.ainovel.server.web.dto.PlatformPublishBindRequest;
import com.ainovel.server.web.dto.PlatformPublishDraftRequest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * 第三方网文平台「官方投稿 / 作品同步」网关 — 当前为<strong>接口预留</strong>，不调用任何外网平台。
 * <p>
 * 后续实装建议：
 * <ul>
 *   <li>按 platformId 路由到独立 Adapter（阅文、番茄等），统一实现 {@code PlatformPublishGateway}</li>
 *   <li>密钥与 token 放安全配置或按用户加密存储，禁止写死在代码中</li>
 *   <li>提交正文前做敏感词/格式校验与限流</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/v1/platform-publish")
@Slf4j
@Tag(name = "PlatformPublish", description = "平台官方投稿网关（预留）")
public class PlatformPublishController {

    public static final String ERROR_NOT_IMPLEMENTED = "PLATFORM_PUBLISH_NOT_IMPLEMENTED";
    public static final String STATUS_STUB = "STUB";

    private void requireLogin(CurrentUser user) {
        if (user == null || user.getId() == null || user.getId().isBlank()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        }
    }

    @GetMapping("/capabilities")
    @Operation(summary = "查询平台投稿能力清单", description = "返回已规划的平台及是否已对接（当前均为预留）")
    public Mono<ApiResponse<Map<String, Object>>> capabilities(@AuthenticationPrincipal CurrentUser currentUser) {
        requireLogin(currentUser);
        log.debug("platform-publish capabilities userId={}", currentUser.getId());

        Map<String, Object> qidian = new LinkedHashMap<>();
        qidian.put("id", "QIDIAN");
        qidian.put("displayName", "起点中文网");
        qidian.put("bindingSupported", false);
        qidian.put("publishSupported", false);
        qidian.put("docsUrl", "https://open.yuewen.com/");
        qidian.put("remarks", "需阅文开放平台合同与 appId；当前未对接");

        Map<String, Object> fanqie = new LinkedHashMap<>();
        fanqie.put("id", "FANQIE");
        fanqie.put("displayName", "番茄小说");
        fanqie.put("bindingSupported", false);
        fanqie.put("publishSupported", false);
        fanqie.put("docsUrl", "");
        fanqie.put("remarks", "平台接口以官方文档为准；当前未对接");

        Map<String, Object> qimao = new LinkedHashMap<>();
        qimao.put("id", "QIMAO");
        qimao.put("displayName", "七猫中文网");
        qimao.put("bindingSupported", false);
        qimao.put("publishSupported", false);
        qimao.put("docsUrl", "");
        qimao.put("remarks", "当前未对接");

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("implementationStatus", STATUS_STUB);
        data.put("apiVersion", 1);
        data.put("platforms", List.of(qidian, fanqie, qimao));

        return Mono.just(ApiResponse.success("平台官方投稿能力预留，尚未连接实网", data));
    }

    @PostMapping("/bind")
    @Operation(summary = "绑定平台作者账号", description = "OAuth / 授权码交换 token（预留）")
    public Mono<ApiResponse<Void>> bind(
            @AuthenticationPrincipal CurrentUser currentUser,
            @RequestBody(required = false) PlatformPublishBindRequest body) {
        requireLogin(currentUser);
        log.info("platform-publish bind (stub) userId={} platformId={}",
                currentUser.getId(), body != null ? body.getPlatformId() : null);
        return Mono.just(ApiResponse.error(
                "官方账号绑定尚未启用。请使用导出作品后，在各平台作家助手完成投稿。",
                ERROR_NOT_IMPLEMENTED));
    }

    @PostMapping("/drafts")
    @Operation(summary = "提交章节草稿至平台", description = "创建或更新平台侧章节（预留）")
    public Mono<ApiResponse<Void>> submitDraft(
            @AuthenticationPrincipal CurrentUser currentUser,
            @RequestBody(required = false) PlatformPublishDraftRequest body) {
        requireLogin(currentUser);
        log.info("platform-publish drafts (stub) userId={} novelId={} platformId={}",
                currentUser.getId(),
                body != null ? body.getNovelId() : null,
                body != null ? body.getPlatformId() : null);
        return Mono.just(ApiResponse.error(
                "官方投稿通道尚未启用。请使用「导出 TXT / Markdown」后，在平台后台上传。",
                ERROR_NOT_IMPLEMENTED));
    }
}
