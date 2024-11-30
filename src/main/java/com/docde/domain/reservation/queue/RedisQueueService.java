package com.docde.domain.reservation.queue;

import com.docde.domain.reservation.dto.ReservationPatientRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;


@Slf4j
@Service
@RequiredArgsConstructor
public class RedisQueueService {

    private final QueueMetricsService queueMetricsService;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ReservationQueueManager reservationQueueManager;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule()).setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));

    private static final String QUEUE_KEY = "reservationQueue";

    public boolean enqueueRequest(ReservationPatientRequest.CreateReservation request) {
        try {
            String jsonRequest = objectMapper.writeValueAsString(request);
            redisTemplate.opsForList().leftPush(QUEUE_KEY, jsonRequest);
            log.info("큐에 요청을 추가함");

            // TTL 설정: 1분 후 자동 삭제
            redisTemplate.expire(QUEUE_KEY, Duration.ofMinutes(3));
            queueMetricsService.processQueueRequest(true); // 큐 추가 성공

            // Queue Processor 트리거
            reservationQueueManager.triggerQueueProcessor(this); // RedisQueueService를 전달
        } catch (JsonProcessingException e) {
            queueMetricsService.processQueueRequest(false); // 큐 추가 실패
            log.error("요청 직렬화 실패: {}", e.getMessage());
            throw new RuntimeException("직렬화 실패 ", e);
        }
        return false;
    }

    public List<String> dequeueBatch(int batchSize) {

        long queueSize = redisTemplate.opsForList().size(QUEUE_KEY);
        log.info("현재 큐 크기: {}", queueSize);

        int adjustedBatchSize = Math.min(batchSize, (int) queueSize);
        List<Object> jsonRequests = redisTemplate.opsForList().rightPop(QUEUE_KEY, adjustedBatchSize);

        if (jsonRequests.isEmpty()) {
            log.info("디큐할 요청이 없습니다.");
            return List.of();
        }

        log.info("큐에서 {}개의 요청을 디큐함", jsonRequests.size());

        // 요청이 성공적으로 처리된 경우 상태를 기록
        jsonRequests.forEach(request -> {
            try {
                // 직렬화 해제 후 로그 기록 (선택 사항)
                ReservationPatientRequest.CreateReservation deserializedRequest = objectMapper.readValue(request.toString(), ReservationPatientRequest.CreateReservation.class);
                log.info("처리 완료된 요청: {}", deserializedRequest);

                // 메트릭 서비스 업데이트
                queueMetricsService.processQueueRequest(true); // 디큐 성공
            } catch (JsonProcessingException e) {
                log.error("디큐된 요청 직렬화 해제 실패: {}", e.getMessage());
                queueMetricsService.processQueueRequest(false); // 디큐 실패
            }
        });

        return jsonRequests.stream()
                .map(Object::toString)
                .collect(Collectors.toList());
    }
}
