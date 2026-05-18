package com.ainovel.server.web.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ainovel.server.domain.model.User;
import com.ainovel.server.service.UserService;
import com.ainovel.server.service.CreditService;
import com.ainovel.server.web.dto.IdDto;
import com.ainovel.server.web.dto.UserRegistrationRequest;
import com.ainovel.server.web.dto.UserUpdateDto;

import reactor.core.publisher.Mono;

/**
 * 用户控制器
 */
@RestController
@RequestMapping("/api/v1/users")
public class UserController {

    private final UserService userService;
    private final CreditService creditService;

    @Autowired
    public UserController(UserService userService, CreditService creditService) {
        this.userService = userService;
        this.creditService = creditService;
    }

    /**
     * 注册用户
     *
     * @param request 注册请求
     * @return 创建的用户
     */
    @PostMapping("/register")
    public Mono<User> registerUser(@RequestBody UserRegistrationRequest request) {
        User user = User.builder()
                .username(request.getUsername())
                .password(request.getPassword())
                .email(request.getEmail())
                .displayName(request.getDisplayName())
                .build();

        return userService.createUser(user)
            .flatMap(created -> creditService.grantNewUserCredits(created.getId())
                .onErrorResume(err -> Mono.just(false))
                .thenReturn(created)
            );
    }

    /**
     * 获取用户信息 (REST风格)
     *
     * @param id 用户ID
     * @return 用户信息
     */
    @GetMapping("/{id}")
    public Mono<User> getUserById(@PathVariable String id) {
        return userService.findUserById(id);
    }

    /**
     * 获取用户信息 (POST方式，保持向后兼容)
     *
     * @param idDto 包含用户ID的DTO
     * @return 用户信息
     */
    @PostMapping("/get")
    public Mono<User> getUserByIdPost(@RequestBody IdDto idDto) {
        return userService.findUserById(idDto.getId());
    }

    /**
     * 更新用户信息 (REST风格)
     *
     * @param id 用户ID
     * @param user 用户信息
     * @return 更新后的用户
     */
    @PutMapping("/{id}")
    public Mono<User> updateUser(@PathVariable String id, @RequestBody User user) {
        return userService.updateUser(id, user);
    }

    /**
     * 更新用户信息 (POST方式，保持向后兼容)
     *
     * @param userUpdateDto 包含用户ID和更新信息的DTO
     * @return 更新后的用户
     */
    @PostMapping("/update")
    public Mono<User> updateUserPost(@RequestBody UserUpdateDto userUpdateDto) {
        return userService.updateUser(userUpdateDto.getId(), userUpdateDto.getUser());
    }

    /**
     * 删除用户
     *
     * @param idDto 包含用户ID的DTO
     * @return 操作结果
     */
    @PostMapping("/delete")
    public Mono<Void> deleteUser(@RequestBody IdDto idDto) {
        return userService.deleteUser(idDto.getId());
    }
}
