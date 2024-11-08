package com.docde.common.aop;


import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;


@Aspect
@Component
public class LockAspect {

    private final RedissonClient redissonClient; // redisson 라이브러리
    private static final String LOCK_KEY_PREFIX = "reservation-lock:";  // Redis에 저장될 락 키 접두어, 특정 락을 식별하기 위한 문자열


    public LockAspect(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Around(value = "@annotation(Lockable) && args(doctorId, reservationTime, ..)", argNames = "joinPoint,doctorId,reservationTime")
    public Object around(ProceedingJoinPoint joinPoint, Long doctorId, LocalDateTime reservationTime) throws Throwable {
        System.out.println("Lock Aspect Triggered");

        // 예약 시간을 분 단위로 자르고 doctorId와 결합해 lockKey 생성
        String lockKey = LOCK_KEY_PREFIX + doctorId + ":" + reservationTime.truncatedTo(ChronoUnit.MINUTES);
        RLock lock = redissonClient.getFairLock(lockKey);

        // 락 획득 시도
        if (lock.tryLock(10, 30, TimeUnit.SECONDS)) {
            try {
                System.out.println("Lock acquired for key: " + lockKey);

                return joinPoint.proceed(); // 원래 호출된 메서드 실행
            } finally {
                lock.unlock(); // 작업 완료 후 락 해제
                System.out.println("Lock released for key: " + lockKey);

            }
        } else {
            throw new IllegalStateException("락 획득 실패: " + lockKey);
        }
    }


    @Around("@annotation(distributedLock)")  // DistributedLock 어노테이션이 있는 메서드에만 적용
    public Object lockMethod(ProceedingJoinPoint joinPoint, DistributedLock distributedLock) throws Throwable {
        String lockKey = distributedLock.key();  // 어노테이션에 설정한 키 사용
        RLock lock = redissonClient.getLock(lockKey);

        boolean acquired = false;
        try {
            // 지정한 대기 시간과 락 유지 시간을 사용해 락 획득 시도
            acquired = lock.tryLock(distributedLock.waitTime(), distributedLock.leaseTime(), TimeUnit.SECONDS);
            if (acquired) {
                // 락을 획득하면 메서드 실행
                return joinPoint.proceed();
            } else {
                throw new RuntimeException("Unable to acquire lock for key: " + lockKey);
            }
        } finally {
            if (acquired && lock.isHeldByCurrentThread()) {
                // 락을 소유한 경우 락 해제
                lock.unlock();
            }
        }
    }
}