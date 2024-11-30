package com.docde.domain.reservation.service;

import com.docde.common.response.ErrorStatus;
import com.docde.common.aop.Lockable;
import com.docde.common.enums.UserRole;
import com.docde.common.exceptions.ApiException;
import com.docde.domain.auth.entity.AuthUser;
import com.docde.domain.doctor.repository.DoctorRepository;
import com.docde.domain.patient.repository.PatientRepository;
import com.docde.domain.reservation.cache.RedisCacheService;
import com.docde.domain.reservation.dto.ReservationPatientRequest;
import com.docde.domain.reservation.entity.Reservation;
import com.docde.domain.reservation.entity.ReservationStatus;
import com.docde.domain.reservation.queue.RedisQueueService;
import com.docde.domain.reservation.repository.ReservationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReservationPatientService {
    private final ReservationRepository reservationRepository;
    private final RedisQueueService redisQueueService;
    private final RedisCacheService redisCacheService;
    private final ReservationHandler reservationHandler;
    private static final long TRAFFIC_CHECK_PERIOD = 60 * 1000; // 1분 (밀리초 단위)
    private final Queue<Long> requestTimestamps = new ConcurrentLinkedQueue<>();
    private final MeterRegistry meterRegistry;
    private final DoctorRepository doctorRepository;
    private final PatientRepository patientRepository;

    public Reservation createReservation(Long doctorId, LocalDateTime reservationTime, String reservationReason, AuthUser authUser) {

        LocalTime startOfHighTraffic = LocalTime.of(8, 0);
        LocalTime endOfHighTraffic = LocalTime.of(10, 0);
        LocalTime reservationLocalTime = reservationTime.toLocalTime();

        if ((reservationLocalTime.isAfter(startOfHighTraffic) || reservationLocalTime.equals(startOfHighTraffic)) &&
                (reservationLocalTime.isBefore(endOfHighTraffic) || reservationLocalTime.equals(endOfHighTraffic))) {

            ReservationPatientRequest.CreateReservation request = new ReservationPatientRequest.CreateReservation(
                    reservationReason, doctorId, reservationTime, authUser.getPatientId());
            log.info("예약요청 큐가 들어갔는지: {}", request);

            redisQueueService.enqueueRequest(request);
            return null;  // 비동기 큐에 추가되었으므로 null 반환
        }

        // 그 외 시간대는 캐싱+락으로 처리
        return createReservationWithLock(doctorId, reservationTime, reservationReason, authUser);
    }



/*    private boolean HighTraffic() {
        long now = System.currentTimeMillis();

        // 1분이 지난 요청 타임스탬프 제거
        while (!requestTimestamps.isEmpty() && now - requestTimestamps.peek() > TRAFFIC_CHECK_PERIOD) {
            requestTimestamps.poll();
        }

        // 현재 요청 추가
        requestTimestamps.add(now);

        // 초당 요청 속도 계산
        long elapsedTime = Math.max(1, (now - requestTimestamps.peek())); // 첫 요청 이후 경과 시간(ms)
        double requestsPerSecond = (double) requestTimestamps.size() / (elapsedTime / 1000.0);

        // 초당 요청 속도가 특정 기준 이상일 경우 고트래픽으로 판단
        return requestsPerSecond > 2000.0;
    }*/



    @Transactional(isolation = Isolation.SERIALIZABLE)
    @Lockable
    public Reservation createReservationWithLock(Long doctorId, LocalDateTime reservationTime, String reservationReason, AuthUser authUser) {

        String cacheKey = "doctor:" + doctorId + ", availability:" + reservationTime;

        String isAvailableString = (String) redisCacheService.getCachedData(cacheKey);
        Boolean Available = isAvailableString != null ? Boolean.parseBoolean(isAvailableString) : null;


        // null이 아니라면, 캐시에서 어떤값이 설정되어있다는 의미
        // !Available가 true라면(Available이 true라면) 예약이 불가능
        if (Available != null && !Available) {
            throw new ApiException(ErrorStatus._DUPLICATE_RESERVATION);
        }

        // 캐시에서 예약 가능 여부를 false로 설정
        redisCacheService.cacheData(cacheKey, String.valueOf(false), 30);


        // 실제 예약 핸들러 호출
        Reservation reservation = reservationHandler.handleReservation(doctorId, reservationTime, reservationReason, authUser.getPatientId());

        return reservation;
    }

//    /**
//     * 예약 생성 로직
//     *
//     * @param doctorId          예약할 의사의 ID
//     * @param reservationTime   예약 시간
//     * @param reservationReason 예약 사유
//     * @param authUser          인증된 사용자 정보
//     * @return 생성된 예약 엔티티
//     */
/*    public Reservation createReservation(Long doctorId, LocalDateTime reservationTime, String reservationReason, AuthUser authUser) {
        // 의사 정보 가져오기
        Doctor doctor = doctorRepository.findById(doctorId)
                .orElseThrow(() -> new IllegalArgumentException("Doctor not found with ID: " + doctorId));

        // 환자 정보 가져오기
        Long patientId = authUser.getPatientId(); // AuthUser에서 환자 ID 가져오기
        Patient patient = patientRepository.findById(patientId)
                .orElseThrow(() -> new IllegalArgumentException("Patient not found with ID: " + patientId));

        // 예약 생성
        Reservation reservation = Reservation.builder()
                .status(ReservationStatus.WAITING_RESERVATION)
                .reservationTime(reservationTime)
                .reservationReason(reservationReason)
                .doctor(doctor)
                .patient(patient)
                .build();

        // 예약 저장
        return reservationRepository.save(reservation);
    }*/



/*    private boolean HighTraffic() {
        long now = System.currentTimeMillis();

        // 1분이 지난 요청 타임스탬프 제거
        while (!requestTimestamps.isEmpty() && now - requestTimestamps.peek() > TRAFFIC_CHECK_PERIOD) {
            requestTimestamps.poll();
        }

        // 현재 요청 추가
        requestTimestamps.add(now);

        // 초당 요청 속도 계산
        long elapsedTime = Math.max(1, (now - requestTimestamps.peek())); // 첫 요청 이후 경과 시간(ms)
        double requestsPerSecond = (double) requestTimestamps.size() / (elapsedTime / 1000.0);

        // 초당 요청 속도가 특정 기준 이상일 경우 고트래픽으로 판단
        return requestsPerSecond > 2000.0;
    }*/






    @Transactional
    public Reservation cancelReservation(Long reservationId, AuthUser authUser) {
        Reservation reservation = reservationRepository.findByIdWithDoctorAndHospitalAndPatient(reservationId).orElseThrow(() -> new ApiException(ErrorStatus._NOT_FOUND_RESERVATION));
        if (!reservation.getPatient().getId().equals(authUser.getPatientId()))
            throw new ApiException(ErrorStatus._FORBIDDEN);

        if (reservation.getStatus() == ReservationStatus.RESERVATION_CANCELED) {
            throw new ApiException(ErrorStatus._ALREADY_CANCEL_RESERVATION);
        } else if (reservation.getStatus() == ReservationStatus.DONE) {
            throw new ApiException(ErrorStatus._ALREADY_DONE_RESERVATION);
        } else if (reservation.getStatus() == ReservationStatus.RESERVATION_DENIED) {
            throw new ApiException(ErrorStatus._DENIED_RESERVATION);
        }

        reservation.setStatus(ReservationStatus.RESERVATION_CANCELED);
        return reservationRepository.save(reservation);
    }

    public Reservation getReservation(Long reservationId, AuthUser authUser) {
        Reservation reservation = reservationRepository.findByIdWithDoctorAndHospitalAndPatient(reservationId).orElseThrow(() -> new ApiException(ErrorStatus._NOT_FOUND_RESERVATION));
        if (authUser.getUserRole().equals(UserRole.ROLE_DOCTOR) || authUser.getUserRole().equals(UserRole.ROLE_DOCTOR_PRESIDENT)) {
            if (authUser.getHospitalId() == null || !reservation.getDoctor().getHospital().getId().equals(authUser.getHospitalId()))
                throw new ApiException(ErrorStatus._FORBIDDEN_DOCTOR_NOT_BELONG_TO_HOSPITAL);
        } else {
            if (!reservation.getPatient().getId().equals(authUser.getPatientId()))
                throw new ApiException(ErrorStatus._FORBIDDEN);
        }

        return reservation;
    }
}