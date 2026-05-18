package com.ainovel.server.web.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.ainovel.server.domain.model.CommunityReply;
import com.ainovel.server.domain.model.CommunityTopic;
import com.ainovel.server.security.CurrentUser;
import com.ainovel.server.service.CommunityDiscussionService;
import com.ainovel.server.web.dto.CommunityTopicPageResponse;
import com.ainovel.server.web.dto.CommunityUserStatsResponse;
import com.ainovel.server.web.dto.CreateCommunityReplyRequest;
import com.ainovel.server.web.dto.CreateCommunityTopicRequest;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 社区讨论 API
 */
@RestController
@RequestMapping("/api/v1/community")
@RequiredArgsConstructor
public class CommunityDiscussionController {

    private final CommunityDiscussionService communityDiscussionService;

    @GetMapping("/topics")
    public Mono<CommunityTopicPageResponse> listTopics(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "latest") String sort,
            @RequestParam(required = false) String q) {
        return communityDiscussionService.listTopics(page, size, sort, q);
    }

    /** 与 `/topics/{id}` 区分，避免 id 被解析为 "trending" */
    @GetMapping("/trending")
    public Mono<List<CommunityTopic>> trendingTopics(@RequestParam(defaultValue = "10") int size) {
        return communityDiscussionService.listTrending(size);
    }

    @GetMapping("/me/stats")
    public Mono<CommunityUserStatsResponse> myStats(@AuthenticationPrincipal CurrentUser currentUser) {
        if (currentUser == null || currentUser.getId() == null) {
            return Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录"));
        }
        return communityDiscussionService.userStats(currentUser.getId());
    }

    @GetMapping("/topics/{id}")
    public Mono<CommunityTopic> getTopic(@PathVariable String id) {
        return communityDiscussionService.getTopic(id);
    }

    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<CommunityTopic> createTopic(
            @AuthenticationPrincipal CurrentUser currentUser,
            @RequestBody CreateCommunityTopicRequest request) {
        return communityDiscussionService.createTopic(currentUser, request);
    }

    @GetMapping("/topics/{id}/replies")
    public Flux<CommunityReply> listReplies(
            @PathVariable("id") String topicId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {
        return communityDiscussionService.listReplies(topicId, page, size);
    }

    @PostMapping("/topics/{id}/replies")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<CommunityReply> createReply(
            @AuthenticationPrincipal CurrentUser currentUser,
            @PathVariable("id") String topicId,
            @RequestBody CreateCommunityReplyRequest request) {
        return communityDiscussionService.createReply(currentUser, topicId, request);
    }

    @PostMapping("/topics/{id}/same-question")
    public Mono<CommunityTopic> sameQuestion(@PathVariable("id") String topicId) {
        return communityDiscussionService.incrementSameQuestion(topicId);
    }
}
