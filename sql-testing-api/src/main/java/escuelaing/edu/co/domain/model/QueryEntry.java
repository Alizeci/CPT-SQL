package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class QueryEntry {
    private String queryId;
    private String className;
    private String methodName;
    private String queryDescription;
    private boolean hasReq;
    private long maxResponseTimeMs;
    private String priority;
    private String reqDescription;
    private boolean allowPlanChange;
}