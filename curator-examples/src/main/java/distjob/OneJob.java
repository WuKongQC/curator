package distjob;

/**
 * @author wangjie
 * @date 2020/6/11 17:44
 */
public class OneJob implements  SimpleJob {

    public void execute(int shardItem) throws  Exception{
        System.out.println("start item:" + shardItem);
        try{
            while(true){
                Thread.sleep(10*1000);
                System.out.println("working:" + shardItem);
            }

        }catch (InterruptedException ex){
            System.out.println("interrupted:" + shardItem);
            Thread.interrupted();
            throw new InterruptedException("interrupted");
        }finally {
            System.out.println("end item:" + shardItem);
        }


    }
}
