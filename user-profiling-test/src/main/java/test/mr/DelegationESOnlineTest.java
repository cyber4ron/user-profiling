package test.mr;

import com.lianjia.profiling.batch.mr.Delegation;

import java.io.IOException;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

public class DelegationESOnlineTest {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        new Delegation().bootstrap(args[0], args[1]);
    }
}
