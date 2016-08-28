package db.load;

import com.alibaba.fastjson.JSON;
import com.lianjia.profiling.common.BlockingBackoffRetryProxy;
import db.dao.FollowDao;
import db.domain.FollowInfo;
import org.elasticsearch.action.update.UpdateRequest;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by huangfangsheng on 16/6/12.
 */

public class FollowTimerLoad {

    public static Date lastScanDate = new Date();

    public static void main(String[] args) throws UnknownHostException {
        timer2();
    }

    // 设定指定任务task在指定延迟delay后进行固定延迟peroid的执行
    // schedule(TimerTask task, long delay, long period)
    public static void timer2() throws UnknownHostException {
        final FollowDao dao = new FollowDao();

        final BlockingBackoffRetryProxy proxy = new BlockingBackoffRetryProxy(new scala.collection.immutable.HashMap<String, String>());
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
                Date nowScanDate = new Date();
                try {
                    List<FollowInfo> list = dao.getFollowInfoByMtime(df.format(lastScanDate), df.format(nowScanDate));
                    System.out.println("list.size: " + list.size());
                    for (FollowInfo fl : list) {
                        System.out.println(JSON.toJSONString(fl.toHashMap()));
                        proxy.send(new UpdateRequest(FollowDao.IDX, FollowDao.TYPE, String.valueOf(fl.getFavorite_id()))
                                           .docAsUpsert(true)
                                           .doc(JSON.toJSONString(fl.toHashMap())));
                    }
                    proxy.flush();

                    lastScanDate = nowScanDate;
                } catch (SQLException e) {
                    e.printStackTrace();
                    proxy.flush();
                }
            }
        }, 1000, 1000 * 60);
    }
}

