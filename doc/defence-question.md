# Câu hỏi & Vấn đề có thể bị hỏi tại buổi bảo vệ giải pháp

---

## 1. Câu hỏi về lý do chọn giải pháp

**Q: Tại sao không dùng Resilience4j thay Sentinel?**
> Resilience4j chỉ có OPEN/CLOSED, không có adaptive throttling giảm dần. Khi downstream yếu, cần giảm 50% chứ không phải dừng hẳn. Resilience4j không có cluster mode — mỗi pod limit riêng khiến quota không chính xác khi scale.

**Q: Tại sao không dùng MuleSoft Rate Limiting Policy thuần?**
> MuleSoft chỉ kiểm soát ở HTTP layer — khi reject request, upstream đã tốn tài nguyên xử lý qua toàn bộ service chain rồi mới fail ở bước cuối. Sentinel chặn ngay tại Kafka consumer trước khi tốn bất kỳ tài nguyên nào.

**Q: Tại sao không dùng KEDA scale down pod?**
> Scale down pod làm External Kafka lag tăng, conflict với HPA hiện tại. Scale mất 30-60s trong khi Sentinel block ngay ~1-2ms. Khi scale up lại có thundering herd — tất cả pod cùng gọi downstream cùng lúc.

**Q: Tại sao không tự build bằng Redis thuần?**
> Về bản chất là tự build lại Sentinel — tốn thời gian, thiếu dashboard, thiếu adaptive logic, thiếu cluster mode chính xác. Sentinel đã giải quyết hết các edge case này qua 10 năm battle-test.

**Q: Sentinel có 703 open issues, tại sao vẫn chọn?**
> 703 issues là bình thường với project lớn (Spring Framework ~1000+, Kafka ~700+). Nhiều issue là câu hỏi sử dụng, không phải bug. Spring Cloud Alibaba vừa release 2025.1.0.0 với Sentinel vẫn là thành phần cốt lõi — project không bị bỏ bê.

---

## 2. Câu hỏi về kiến trúc

**Q: Token Server là SPOF — nếu nó chết thì sao?**
> Sentinel có `fallbackToLocalWhenFail = true`. Khi Token Server chết, mỗi pod tự limit theo local rule (VD: 250 req/s/pod × 20 pod = 5000 req/s). Kém chính xác hơn nhưng vẫn an toàn hơn hiện tại vì hiện tại không có gì kiểm soát cả.

**Q: Local limit 250/pod có đúng không khi HPA scale lên 40 pod?**
> 40 pod × 250 = 10000 req/s — vượt ngưỡng 5000. Đây là trade-off chấp nhận được vì: (1) Token Server chết là hiếm, (2) HPA scale lên 40 pod cùng lúc Token Server chết là xác suất rất thấp, (3) vẫn tốt hơn hiện tại không có limit gì.

**Q: Kafka consumer không commit offset — consumer group có bị rebalance không?**
> Có rủi ro nếu lag quá lớn và kéo dài vượt `max.poll.interval.ms`. Cần config đủ lớn hoặc implement heartbeat riêng. Khi downstream chỉ DEGRADED (không DOWN), lag không kéo dài đủ để trigger rebalance.

**Q: Rule lưu ở đâu, restart Token Server có mất rule không?**
> Rule in-memory — restart mất rule. Giải pháp: lưu rule vào ConfigMap K8s hoặc Nacos. Token Server load rule từ đó khi khởi động. Đây là việc cần làm trước khi production.

**Q: Nhiều Service Order (1-7) cùng xin token từ 1 Token Server — Token Server có bị nghẽn không?**
> Token Server dùng Netty async, mỗi pod giữ 1 TCP persistent connection. Với 2000 tx/s của hệ thống, Token Server chỉ nhận ~2000 req/s — hoàn toàn trong ngưỡng. Token Server 512MB RAM, 0.5 CPU là đủ.

**Q: Tại sao không dùng Redis thay Token Server cho cluster quota?**
> Redis dùng INCR atomic nhưng cần 2 round-trip (get + set) hoặc Lua script. Sentinel Token Server dùng sliding window in-memory với TCP persistent connection — latency thấp hơn (~1ms vs ~3-5ms qua Redis). Với 2000 tx/s, khác biệt nhỏ nhưng Token Server đơn giản hơn về code phía client.

---

## 3. Câu hỏi về vận hành

**Q: Làm thế nào biết downstream đang yếu để update rule?**
> Prometheus scrape metrics từ MuleSoft/service cuối: error rate (429/503/timeout) và p99 latency. Grafana alert khi error_rate > 10% trong 30s → webhook → update Sentinel rule. Không cần downstream expose gì thêm.

**Q: Nếu Grafana alert sai (false positive) thì sao — quota giảm oan?**
> Đây là rủi ro của Phase 2. Giải pháp: tune threshold cẩn thận dựa trên traffic thực tế, dùng `for: 30s` để alert chỉ fire khi sustained, không phải spike ngắn. Có thể implement manual override qua Dashboard.

**Q: Làm sao rollback nếu Sentinel gây vấn đề?**
> Dynamic rule — chỉ cần update rule về count rất cao (VD: 999999) hoặc tắt cluster mode. Không cần restart pod, rollback trong vài giây.

**Q: Exception job đang retry — có bị ảnh hưởng không?**
> Exception job phải check Sentinel circuit state trước khi retry. Nếu downstream đang CRITICAL → skip, không bắn. Dùng exponential backoff theo loại lỗi. Đây là một phần quan trọng cần implement trong Phase 1.

**Q: Monitoring Sentinel như thế nào?**
> Sentinel Dashboard expose metrics (QPS, RT, error rate) per resource per pod. Có thể scrape vào Prometheus qua Sentinel Prometheus exporter và hiển thị trên Grafana đang có sẵn.

---

## 4. Câu hỏi về tính đúng đắn của giải pháp

**Q: Shared quota 5000 req/s — con số này lấy từ đâu?**
> Downstream capacity bình thường ~5000 req/s. Con số cụ thể cần được downstream team confirm và test thực tế. Sentinel rule có thể update dynamic nên có thể bắt đầu conservative (VD: 3000) rồi tăng dần.

**Q: Upstream A, B, C khác cũng gọi downstream — làm sao đảm bảo tổng không vượt 5000?**
> Phase 3: tất cả upstream cùng xin token từ 1 Sentinel Token Server chung, hoặc dùng MuleSoft SLA Policy. Phase 1 chỉ giải quyết upstream của team bạn trước — đây là improvement so với không có gì, không phải giải pháp hoàn hảo ngay.

**Q: Nếu downstream recover, hệ thống có tự tăng quota về bình thường không?**
> Có — Grafana alert clear khi error_rate < 5% trong 60s → webhook update quota về 5000. Sentinel Degrade Rule cũng có HALF_OPEN state — cho một lượng nhỏ request qua test trước khi CLOSED hoàn toàn.

**Q: Idempotency — message retry có bị duplicate không?**
> Hiện tại có rủi ro duplicate khi retry. Kafka consumer không commit offset → message được xử lý lại từ đầu. Cần đảm bảo các bước trong service chain idempotent, hoặc dùng idempotency key khi gọi downstream. Đây là vấn đề hiện tại, không phải vấn đề do Sentinel gây ra.

---

## 5. Câu hỏi về non-Java service

**Q: Service không dùng Java có dùng Sentinel được không?**
> Sentinel có SDK cho Go và C++ nhưng ecosystem yếu hơn Java nhiều, không nên dùng. Giải pháp: service non-Java đọc circuit state từ Redis (do Java service/Sentinel ghi vào), tự kiểm soát Kafka commit offset theo state đó. Không cần Sentinel SDK.

**Q: Java service ghi Redis state lúc nào, tần suất ra sao?**
> Sentinel callback khi circuit state thay đổi (CLOSED → DEGRADED → CRITICAL → CLOSED). Không ghi liên tục — chỉ ghi khi có thay đổi. TTL Redis key đủ dài (VD: 5 phút) để non-Java service không bị stale data.

---

## 6. Câu hỏi về phạm vi và lộ trình

**Q: Phase 1 giải quyết được bao nhiêu % bài toán?**
> ~70%. Sentinel tại service cuối bảo vệ downstream khỏi bị dồn tải từ upstream của bạn. Exception job không bắn ồ ạt. Chưa giải quyết shared quota với upstream B, C — nhưng đây là improvement lớn so với không có gì.

**Q: Phase 1 mất bao lâu để implement?**
> Ước tính 2-3 sprint: (1) deploy Token Server + Dashboard, (2) thêm Sentinel library + wrap consumer, (3) fix exception job. Không phá vỡ hệ thống hiện có vì chỉ thêm library và deploy thêm pod.

**Q: Nếu downstream team không muốn phối hợp thì sao?**
> Phase 1 và 2 không cần downstream làm gì. Sentinel tự detect 429/503/latency từ response hiện tại. Phase 3 (shared quota với B, C) cần phối hợp với các upstream team khác — không phải downstream.

**Q: Tại sao không làm Phase 3 ngay từ đầu?**
> Phase 3 cần phối hợp tất cả upstream A, B, C — tốn thời gian đàm phán, align. Phase 1 team bạn tự làm được ngay, deliver value sớm, giảm rủi ro trong khi chờ các team khác sẵn sàng.

---

## 7. Câu hỏi về rủi ro và worst case

**Q: Worst case xảy ra là gì?**
> Token Server chết + HPA scale lên nhiều pod cùng lúc → local fallback không đủ → downstream vẫn bị dồn. Xác suất thấp, và vẫn tốt hơn hiện tại (không có gì). Mitigation: deploy Token Server HA (2 pod active-standby).

**Q: Sentinel có phù hợp với môi trường banking/BIAN không?**
> Apache 2.0 license, không có vendor lock-in. Đã được dùng ở nhiều fintech lớn (Bairong Financial Services, iQiyi). Sentinel chạy trong cluster K8s của bạn — không có data nào ra ngoài. Compliance cần review license và security team approve.

**Q: Nếu sau này bỏ Sentinel thì sao?**
> Sentinel chỉ là wrapper (`@SentinelResource` annotation) quanh business logic. Bỏ Sentinel chỉ cần xóa annotation và dependency — business logic không thay đổi. Migration cost thấp.