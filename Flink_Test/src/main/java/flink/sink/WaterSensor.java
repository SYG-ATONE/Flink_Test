package flink.sink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 水位传感器：用于接收水位数据
 *
 * id:传感器编号
 * ts:时间戳
 * vc:水位
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;

    public String getId() {
        return id;
    }

    public Long getTs() {
        return ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }
}
