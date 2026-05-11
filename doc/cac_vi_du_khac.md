Bằng chứng thực tế
Các công ty lớn đang dùng:
Netflix:   concurrency-limits library — production, update Dec 2025
Lyft:      Envoy Adaptive Concurrency — built-in, đang maintain
Google:    gRPC adaptive throttling — built into gRPC framework
Amazon:    SQS backpressure built-in, Lambda reserved concurrency
Alibaba:   Sentinel — Double 11 2024, hàng tỷ req
Xu hướng 2024-2025:
Kết hợp backpressure và rate limiting đang được áp dụng rộng rãi trong microservices, streaming analytics và integration layers — cho thấy tính phổ biến ngày càng tăng của giải pháp này. GitHub
Service mesh như Istio và Linkerd mở rộng khả năng rate limiting xuyên suốt kiến trúc microservice, enforce limits nhất quán giữa các service — đây là hướng đang được nhiều công ty áp dụng ở production. GitHub

Giải pháp của bạn có gì mới không?
Không — đây là pattern chuẩn của industry:
Backpressure:     pattern cũ, proven từ TCP (1980s)
Kafka buffer:     standard trong event-driven (2010s)
Circuit breaker:  Netflix Hystrix (2012), Resilience4j
Sentinel:         Alibaba production (2012→nay)
AIMD:             TCP congestion control (1988→nay)
Trong microservices, backpressure được dùng để xử lý luồng request giữa các service — khi downstream bị overwhelm, HTTP 429 hoặc circuit breaker được dùng để báo hiệu upstream giảm tốc độ. Đây là pattern phổ biến và được chấp nhận rộng rãi trong industry. GitHub

Điểm khác biệt của hệ thống bạn
Pattern chuẩn:   phổ biến
Bài toán của bạn: phức tạp hơn vì:
→ Có MuleSoft làm integration layer (nhiều instance)
→ Kafka ở upstream (không commit offset)
→ Nhiều upstream team cùng gọi 1 downstream
→ Banking system → yêu cầu audit, simplicity cao

→ Giải pháp của bạn kết hợp đúng các pattern chuẩn
cho đặc thù hệ thống banking

Kết luận cho buổi trình bày
"Giải pháp này không phải mới hay thử nghiệm.
Netflix, Lyft, Alibaba, Amazon đã dùng các
pattern tương tự ở production hàng tỷ request/ngày.
Chúng tôi áp dụng đúng pattern đó cho đặc thù
của hệ thống banking với MuleSoft và Kafka."