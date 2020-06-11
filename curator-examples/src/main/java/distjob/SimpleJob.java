package distjob;

public interface SimpleJob  {

    /**
     *
     * @param shardItem
     */
    void execute(int shardItem) throws  Exception;
}
