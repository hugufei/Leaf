package com.sankuai.inf.leaf.snowflake;

import com.google.common.base.Preconditions;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 41位时间戳 + 10位机器号 + 12位序列号
 * 1) 最多支持1023台机器
 * 2) 每ms支持4095增量，1s的话就是409W次【其实这么高的并发也没啥用】
 * 3)
 */
public class SnowflakeIDGenImpl implements IDGen {

    @Override
    public boolean init() {
        return true;
    }

    static private final Logger LOGGER = LoggerFactory.getLogger(SnowflakeIDGenImpl.class);

    private final long twepoch = 1288834974657L;

    // 10位机器id
    private final long workerIdBits = 10L;
    // 1023, 最大的机器id，可以理解为分布式id服务器的台数
    private final long maxWorkerId = -1L ^ (-1L << workerIdBits);//最大能够分配的workerid =1023

    //序列号尾数，每毫秒 生成 2^12次方的个数
    private final long sequenceBits = 12L;

    private final long workerIdShift = sequenceBits;

    //序列号位+机器号位一共22位，剩下的 64-22-1 = 41位为时间戳位
    private final long timestampLeftShift = sequenceBits + workerIdBits;

    //4095 = 4096-1 = 2^12 - 1
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);

    private long workerId;
    private long sequence = 0L;
    private long lastTimestamp = -1L;
    public boolean initFlag = false;
    private static final Random RANDOM = new Random();

    private int port;

    /**
     * zkAddress: 192.168.102.216:2181
     * port: 指定的端口
     * @param zkAddress
     * @param port
     */
    public SnowflakeIDGenImpl(String zkAddress, int port) {
        this.port = port;
        SnowflakeZookeeperHolder holder = new SnowflakeZookeeperHolder(Utils.getIp(), String.valueOf(port), zkAddress);
        // 获取workerId
        initFlag = holder.init();
        if (initFlag) {
            workerId = holder.getWorkerID();
            LOGGER.info("START SUCCESS USE ZK WORKERID-{}", workerId);
        } else {
            Preconditions.checkArgument(initFlag, "Snowflake Id Gen is not init ok");
        }
        Preconditions.checkArgument(workerId >= 0 && workerId <= maxWorkerId, "workerID must gte 0 and lte 1023");
    }

    //生成id
    @Override
    public synchronized Result get(String key) {

        // 获取当前时间戳
        long timestamp = getCurrentTimeMillis();

        // 出现时钟回拨
        if (timestamp < lastTimestamp) {
            // 计算差值
            long offset = lastTimestamp - timestamp;
            // 如果出现时钟回拨的差值小于5ms，则等待，否则快速失败
            // 最大容忍5ms
            if (offset <= 5) {
                try {
                    // 等待一段时间
                    wait(offset << 1);
                    // 重试+快速失败
                    timestamp = getCurrentTimeMillis();
                    if (timestamp < lastTimestamp) {
                        return new Result(-1, Status.EXCEPTION);
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("wait interrupted");
                    return new Result(-2, Status.EXCEPTION);
                }
            } else {
                return new Result(-3, Status.EXCEPTION);
            }
        }

        // 如果一样
        if (lastTimestamp == timestamp) {
            // 当前序列号+1，并且有没有超过最大生成序列号
            sequence = (sequence + 1) & sequenceMask;
            // 超出了则等下1ms
            if (sequence == 0) {
                // seq 为0的时候表示是下一毫秒时间开始对seq做随机
                sequence = RANDOM.nextInt(100);
                // 等到下一个ms
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            // 如果是新的ms，那么重新开始
            sequence = RANDOM.nextInt(100);
        }
        // 记录最大时间
        lastTimestamp = timestamp;
        // 生成id
        // 1) 时间戳左移41位
        // 2) 机器号id左移12位
        long id = ((timestamp - twepoch) << timestampLeftShift) | (workerId << workerIdShift) | sequence;

        // 返回最终结果
        return new Result(id, Status.SUCCESS);
    }

    // 可以理解为等到下一个ms
    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = getCurrentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = getCurrentTimeMillis();
        }
        return timestamp;
    }

    protected long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

    public long getWorkerId() {
        return workerId;
    }

}
