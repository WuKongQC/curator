package distjob;

/**
 * @author wangjie
 * @date 2020/6/11 17:44
 */
public class OneJob implements  SimpleJob {

    public void execute(int shardItem) throws  Exception{
        System.out.println("start item:" + shardItem);
        try{
            Thread.sleep(10*1000);
        }catch (InterruptedException ex){
            Thread.interrupted();
            throw new InterruptedException("interrupted");
        }finally {
            System.out.println("end item:" + shardItem);
        }


    }
}
