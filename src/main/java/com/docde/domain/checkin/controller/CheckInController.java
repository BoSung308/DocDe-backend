package com.docde.domain.checkin.controller;

import com.docde.common.response.ApiResponse;
import com.docde.domain.auth.entity.AuthUser;
import com.docde.domain.checkin.dto.CheckInRequest;
import com.docde.domain.checkin.dto.CheckInResponse;
import com.docde.domain.checkin.dto.CheckInResponseOfPatient;
import com.docde.domain.checkin.queue.service.QueueService;
import com.docde.domain.checkin.service.CheckInService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RequiredArgsConstructor
@RestController
@RequestMapping("/hospitals")
public class CheckInController {

    private final CheckInService checkInService;
    private final QueueService queueService;

    // 접수하기
    @PostMapping("/{hospitalId}/checkin")
    public ResponseEntity<ApiResponse<CheckInResponse>> saveCheckIn(
            @AuthenticationPrincipal AuthUser authUser,
            @PathVariable Long hospitalId,
            @RequestBody CheckInRequest checkInRequest
    ) {
        Long patientId = authUser.getPatientId();
        Long userId = authUser.getId();
        if (queueService.processRequest(authUser, hospitalId, checkInRequest)) {
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(ApiResponse.onCreated(checkInService
                            .saveCheckIn(patientId, userId, hospitalId, checkInRequest)));
        } else {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(ApiResponse.createError("사용자가 너무 많아 서비스가 지연되고 있습니다.", 429));
        }
    }

    // 자신의 접수 상태 확인(사용자)
    @GetMapping("{hospitalId}/checkin")
    public ResponseEntity<ApiResponse<CheckInResponseOfPatient>> getMyCheckIn(
            @AuthenticationPrincipal AuthUser authUser,
            @PathVariable Long hospitalId
    ) {
        return ApiResponse.onSuccess(checkInService.getMyCheckIn(authUser, hospitalId)).toEntity();
    }

    // 접수 목록만 확인(병원)
    @GetMapping("/{hospitalId}/checkin/simple")
    public ResponseEntity<ApiResponse<List<Object>>> getQueue(
            @AuthenticationPrincipal AuthUser authUser,
            @PathVariable Long hospitalId
    ) {
        return ApiResponse.onSuccess(checkInService.getQueue(authUser, hospitalId)).toEntity();
    }

    // 접수 상태 확인(병원)
    @GetMapping("/{hospitalId}/checkin/all")
    public ResponseEntity<ApiResponse<List<CheckInResponse>>> getAllCheckIns(
            @AuthenticationPrincipal AuthUser authUser,
            @PathVariable Long hospitalId
    ) {
        return ApiResponse.onSuccess(checkInService.getAllCheckIns(authUser, hospitalId)).toEntity();
    }

    // 접수 상태 변경
    @PutMapping("/{hospitalId}/checkin/{checkInId}")
    public ResponseEntity<ApiResponse<CheckInResponse>> updateCheckIn(
            @AuthenticationPrincipal AuthUser authUser,
            @PathVariable Long hospitalId,
            @PathVariable Long checkInId,
            @RequestBody CheckInRequest checkInRequest
    ) {
        return ApiResponse.onSuccess(
                checkInService.updateCheckIn(authUser, hospitalId, checkInId, checkInRequest)).toEntity();
    }

    // 접수 번호 초기화
    @PutMapping("/{hospitalId}/checkin/reset")
    public ResponseEntity resetNumber(
            @AuthenticationPrincipal AuthUser authUser,
            @PathVariable Long hospitalId
    ) {
        checkInService.resetNumber(authUser, hospitalId);
        return ResponseEntity.noContent().build();
    }

    // 접수 기록 영구 삭제
    @DeleteMapping("/checkin/{checkInId}")
    public ResponseEntity deleteCheckIn(
            @AuthenticationPrincipal AuthUser authUser,
            @PathVariable Long checkInId
    ) {
        checkInService.deleteCheckIn(authUser, checkInId);
        return ResponseEntity.noContent().build();
    }

}
