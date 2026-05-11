package com.example.sentinel;

import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterFlowRuleManager;
import com.alibaba.csp.sentinel.cluster.server.ClusterTokenServer;
import com.alibaba.csp.sentinel.cluster.server.SentinelDefaultTokenServer;
import com.alibaba.csp.sentinel.cluster.server.config.ClusterServerConfigManager;
import com.alibaba.csp.sentinel.cluster.server.config.ServerFlowConfig;
import com.alibaba.csp.sentinel.cluster.server.config.ServerTransportConfig;
import com.alibaba.csp.sentinel.property.DynamicSentinelProperty;
import com.alibaba.csp.sentinel.property.SentinelProperty;
import com.alibaba.csp.sentinel.slots.block.ClusterRuleConstant;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.ClusterFlowConfig;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.util.function.Function;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootApplication
@RestController
public class SentinelServerApplication {

    private static final int TOKEN_SERVER_PORT = 18730;
    private static final String RESOURCE = "downstream-call";
    private static final long FLOW_ID = 1L;

    @Value("${sentinel.namespace:service-order}")
    private String namespace;

    private final Map<String, Double> limitMap = new ConcurrentHashMap<String, Double>();
    private final Map<String, DynamicSentinelProperty<List<FlowRule>>> propertyMap =
            new ConcurrentHashMap<String, DynamicSentinelProperty<List<FlowRule>>>();

    public static void main(String[] args) {
        // Port 8720: Sentinel internal command center (Dashboard communication)
        System.setProperty("csp.sentinel.api.port", "8720");
        // Port 8721: Spring Boot REST API (admin endpoints)
        SpringApplication.run(SentinelServerApplication.class, args);
    }

    @PostConstruct
    public void startTokenServer() throws Exception {
        // Cấu hình Netty transport port (clients kết nối vào đây)
        ClusterServerConfigManager.loadGlobalTransportConfig(
                new ServerTransportConfig().setPort(TOKEN_SERVER_PORT).setIdleSeconds(600)
        );

        // Cấu hình max QPS toàn server
        ClusterServerConfigManager.loadGlobalFlowConfig(
                new ServerFlowConfig().setMaxAllowedQps(50000.0)
        );

        // Khai báo namespace được phục vụ
        ClusterServerConfigManager.loadServerNamespaceSet(
                new HashSet<String>(Collections.singletonList(namespace))
        );

        // Supplier: khi có namespace mới kết nối → trả property để push rules
        ClusterFlowRuleManager.setPropertySupplier(
                new Function<String, SentinelProperty<List<FlowRule>>>() {
                    @Override
                    public SentinelProperty<List<FlowRule>> apply(String ns) {
                        DynamicSentinelProperty<List<FlowRule>> prop =
                                new DynamicSentinelProperty<List<FlowRule>>();
                        propertyMap.put(ns, prop);
                        double limit = limitMap.containsKey(ns) ? limitMap.get(ns) : 250.0;
                        prop.updateValue(buildRules(limit));
                        return prop;
                    }
                }
        );

        limitMap.put(namespace, 250.0);

        // Start Netty Token Server
        ClusterTokenServer tokenServer = new SentinelDefaultTokenServer();
        tokenServer.start();

        System.out.println("=== Sentinel Token Server started: Netty port=" + TOKEN_SERVER_PORT
                + ", REST port=8720 ===");
    }

    /**
     * AIMD controller của service-order gọi vào đây để cập nhật global quota.
     */
    @PostMapping("/admin/limit")
    public ResponseEntity<String> updateLimit(
            @RequestParam double limit,
            @RequestParam(defaultValue = "service-order") String ns) {
        limitMap.put(ns, limit);
        DynamicSentinelProperty<List<FlowRule>> prop = propertyMap.get(ns);
        if (prop != null) {
            prop.updateValue(buildRules(limit));
        }
        return ResponseEntity.ok("ns=" + ns + " limit=" + limit);
    }

    @GetMapping("/admin/status")
    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("tokenServerPort", TOKEN_SERVER_PORT);
        m.put("limits", limitMap);
        m.put("connectedNamespaces", propertyMap.keySet());
        return m;
    }

    private List<FlowRule> buildRules(double qps) {
        FlowRule rule = new FlowRule();
        rule.setResource(RESOURCE);
        rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        rule.setCount(qps);
        rule.setClusterMode(true);

        ClusterFlowConfig clusterConfig = new ClusterFlowConfig();
        clusterConfig.setFlowId(FLOW_ID);
        clusterConfig.setThresholdType(ClusterRuleConstant.FLOW_THRESHOLD_GLOBAL);
        clusterConfig.setFallbackToLocalWhenFail(true);
        rule.setClusterConfig(clusterConfig);

        return Collections.singletonList(rule);
    }
}
