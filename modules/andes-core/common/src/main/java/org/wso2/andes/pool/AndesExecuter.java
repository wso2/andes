package org.wso2.andes.pool;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

public class AndesExecuter {

    private static Log log = LogFactory.getLog(AndesExecuter.class);
    private static boolean isDebugEnabled = log.isDebugEnabled();
    private static ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("AndesExecutor-%d").build();

    private static ExecutorService executorService = null;

    private volatile static AndesExecuter INSTANCE;

    private AndesExecuter(){}

    private AndesExecuter(int poolSize){
        executorService = Executors.newFixedThreadPool(poolSize, namedThreadFactory);
    }

    public static AndesExecuter getInstance(int poolSize){
        if (INSTANCE == null){
            synchronized (AndesExecuter.class){
                if(INSTANCE == null){
                    INSTANCE = new AndesExecuter(poolSize);
                }
            }
        }
        return INSTANCE;
    }

    static {
        if (isDebugEnabled) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            int workqueueSize = ((ThreadPoolExecutor) executorService).getQueue().size();
                            log.debug("AndesExecuter pool queue size " + workqueueSize);
                            Thread.sleep(30000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }
    }

    public Future<?> submit(Runnable job, String channelId) {
        return executorService.submit(job);
    }

    private static ExecutorService genricExecutorService = Executors.newFixedThreadPool(100, namedThreadFactory);

    public static void runAsync(Runnable runnable){
        genricExecutorService.submit(runnable);
    }

}
