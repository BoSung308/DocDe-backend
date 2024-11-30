package com.docde.domain.reservation.queue;

import com.docde.domain.reservation.dto.ReservationPatientRequest;
import com.docde.domain.reservation.service.ReservationHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReservationQueueConsumer {

    private final ReservationHandler reservationHandler;
    private final QueueMetricsService queueMetricsService;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(12); // 병렬 워커
    private boolean isProcessorRunning = false; // 상태 플래그

    public synchronized void startQueueProcessor(RedisQueueService redisQueueService) {

        if (!isProcessorRunning) {
            isProcessorRunning = true;

            // 고정된 병렬 워커 스레드 풀 사용
            int numWorkers = 64; // 병렬 워커 수
            for (int i = 0; i < numWorkers; i++) {
                executorService.submit(() -> {
                    while (isProcessorRunning) {
                        try {
                            processQueue(250, redisQueueService); // 배치 크기
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt(); // 스레드 인터럽트 처리
                            log.warn("큐 프로세서 스레드 인터럽트");
                        } catch (Exception e) {
                            log.error("JSON 역직렬화 실패: {}", e.getMessage(), e);
                        }
                    }
                });
            }
        }
    }

    public void processQueue(int batchSize, RedisQueueService redisQueueService) {
        List<String> batchRequests = redisQueueService.dequeueBatch(batchSize);

        if (!batchRequests.isEmpty()) {

            batchRequests.forEach(jsonRequest -> {
                try {
                    ReservationPatientRequest.CreateReservation request =
                            objectMapper.readValue(jsonRequest, ReservationPatientRequest.CreateReservation.class);

                    reservationHandler.handleReservation(
                            request.doctorId(),
                            request.reservationTime(),
                            request.reservationReason(),
                            request.patientId()
                    );

                    queueMetricsService.processQueueRequest(true); // 큐 처리 성공

                } catch (JsonProcessingException e) {
                    queueMetricsService.processQueueRequest(false); // 역직렬화 실패
                    log.error("역직렬화 실패: {}", e.getMessage());
                } catch (Exception e) {
                    queueMetricsService.processQueueRequest(false); // 예약 처리 실패

                    log.error("예약 처리 실패: {}", e.getMessage(), e);
                }
            });
        }
    }
}
