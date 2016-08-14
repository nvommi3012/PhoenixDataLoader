
/*PHOENIX DATA LOADER - Loads Data from MS SQL Server to the HBase Cluster Via Phoenix*/

/*Intel Corporation*/

import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class PhoenixDataLoaderMultiThread
{
    private static final Logger LOGGER = LoggerFactory
            .getLogger(PhoenixDataLoaderMultiThread.class);
        
    public static final List<BlockingQueue<ArrayList<HashMap<String,Object>>>> queues = new ArrayList<>(10000);
    
    private static final Map<Class<?>, Parameter> PARAMETERS_MAP = new HashMap<>();
    
    static 
    {
        PARAMETERS_MAP.put(String.class, new StringParameter());
        PARAMETERS_MAP.put(Integer.class, new IntegerParameter());
        PARAMETERS_MAP.put(Long.class, new LongParameter());
        PARAMETERS_MAP.put(Short.class, new ShortParameter());
        PARAMETERS_MAP.put(Byte.class, new ByteParameter());
        PARAMETERS_MAP.put(Double.class, new DoubleParameter());
        PARAMETERS_MAP.put(Timestamp.class, new TimestampParameter());
        PARAMETERS_MAP.put(UUID.class, new UUIDParameter());
        PARAMETERS_MAP.put(Date.class, new DateParameter());
        PARAMETERS_MAP.put(Boolean.class, new BooleanParameter());
        PARAMETERS_MAP.put(byte[].class, new ByteArrayParameter());
    }
    
    public static void main (String[] args)
    {

        
        // Input Num of Threads
        int Insertthreads = 20; 

        StopWatch watch = new StopWatch();
        watch.start();
        
        BlockingQueue<ArrayList<HashMap<String,Object>>> blockingQueue = new ArrayBlockingQueue<>(20000);
        DBReadThread reader = new DBReadThread(1, blockingQueue);
        reader.run();
        queues.add(reader.getReadBlockingQueue());

        ExecutorService executor = Executors.newFixedThreadPool(Insertthreads);
        
        for (int i = 0; i < Insertthreads; i++) 
        {
            InsertThread thread;
            try 
            {
                thread = new InsertThread(i, queues.get(0), reader.getResultSetMetaData(), reader.getprep_template());
                executor.submit(thread);
            } 
            
            catch (Exception e) 
            {
                LOGGER.error("Exception", e);
                executor.shutdownNow();
                System.exit(-1);
                return;
            }
            
        }
        executor.shutdown();
        
        int count = 0;
//        Runtime runtime = Runtime.getRuntime();
        while(!executor.isTerminated())
        {
//            System.out.println("Heap Size Remaining - " + runtime.freeMemory());
//            System.out.println("Awaiting Completion : " + count++);
        }
        
        long timeTaken = watch.getTime();
//        LOGGER.info(String.format("Total Insert Time taken = %d", timeTaken));
	System.out.println("Total Insert Time Taken = " + timeTaken);
    }
    
    
    private static class DBReadThread extends Thread 
    {
        private final int thread_id;
        private final BlockingQueue<ArrayList<HashMap<String,Object>>> ReadblockingQueue;
        ArrayList<HashMap<String,Object>> list = new ArrayList<>();
        Statement sqlServerStmt = null;
        ResultSet sqlServerResultSet = null;
        ResultSetMetaData md = null;
        String prep_template = null;
        
        //Constructor
        private DBReadThread(int i, BlockingQueue<ArrayList<HashMap<String,Object>>> blockingQueue)
        {
            this.thread_id = i;
            this.ReadblockingQueue = blockingQueue;
        }
        
        public BlockingQueue<ArrayList<HashMap<String,Object>>> getReadBlockingQueue()
        {

            return this.ReadblockingQueue;
        }
        
        public ResultSetMetaData getResultSetMetaData()
        {
            return md;
        }
        
        public String getprep_template()
        {
            return prep_template;
        }
        
        @Override
        public void run()
        {
            // Input Batch Size
            int batchSize = 1000;
            
            //Connection
            Connection sqlServerConnection = null;
            sqlServerConnection = sqlconnect();
            

            //Query Records

            try
            {
                sqlServerStmt = sqlServerConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
               sqlServerStmt.setMaxRows(20000);
                String query = "SELECT * FROM WP_EVENTINFOMT";
                sqlServerResultSet = sqlServerStmt.executeQuery(query);
                md = sqlServerResultSet.getMetaData();               
            }
            catch (SQLException e)
            {
                LOGGER.error("Unable to execute DBRead Query by Thread : " + thread_id);
                e.printStackTrace();
            }
            
            
            try
            {
                int columns = md.getColumnCount();
//                sqlServerResultSet.absolute(72500000);
                while (sqlServerResultSet.next()) 
                {
                    
                    StringBuilder queryBuilder = new StringBuilder();
                    StringBuilder valuesBuilder = new StringBuilder();
                    HashMap<String,Object> row = new HashMap<String, Object>(columns);
                    
                    queryBuilder.append("UPSERT INTO ");
                    queryBuilder.append("WP_EVENTINFOMT2");
                    queryBuilder.append(" (");
                                    
                    valuesBuilder.append(" VALUES (");
                    
                    for(int i=1; i<=columns; ++i) 
                    {
                        row.put(md.getColumnName(i),sqlServerResultSet.getObject(i));
                        
                        queryBuilder.append(md.getColumnName(i));
                        queryBuilder.append(",");
                        
                        valuesBuilder.append("?");
                        valuesBuilder.append(",");
                    }
                    queryBuilder.deleteCharAt(queryBuilder.length() - 1);
                    valuesBuilder.deleteCharAt(valuesBuilder.length() - 1);
                    
                    queryBuilder.append(")");
                    queryBuilder.append(valuesBuilder);
                    queryBuilder.append(")");
                    
                    list.add(row);
                    
                    prep_template = queryBuilder.toString();
                    
                    LOGGER.info("Fetched Row : " + list.size());
                    
                    if (list.size() == batchSize) 
                    {
                        LOGGER.info("Inserting into Blocking Queue of thread #" + thread_id);
                        System.out.println("Inserting into Blocking Queue");
			ReadblockingQueue.add(list);
                        list = new ArrayList<HashMap<String,Object>>();
                    } 
                }
                
            }
            catch (Exception e)
            {
                LOGGER.error("Unable to Read Data by Thread : " + thread_id);
                e.printStackTrace();
            }
        }
        
        private Connection sqlconnect()
        {
            Connection connection = null;
            try
            {
            	try 
            	{
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} 
            	catch (ClassNotFoundException e) 
            	{
            		LOGGER.info("SQL Driver Class Not Found");
			e.printStackTrace();
		}

            	connection = DriverManager.getConnection("jdbc:sqlserver://54.201.216.177:1433;databaseName=ePO_DNVPRODePOPOD2;user=sa;password=welcome2intel$");
                System.out.println("Connection Established : SQL SERVER");
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
            return connection;
        }
    }
    
    
    
    private static class InsertThread extends Thread 
    {

        private BlockingQueue<ArrayList<HashMap<String,Object>>> WriteblockingQueue = new ArrayBlockingQueue<ArrayList<HashMap<String,Object>>>(200);
        private ResultSetMetaData md = null;
        public PreparedStatement pstmt = null;
        public String prep_template = null;
        public Connection phoenix_connection = null;
        //Constructor
        private InsertThread(int i, BlockingQueue<ArrayList<HashMap<String,Object>>> blockingQueue, ResultSetMetaData table_md, String str)
        {

//            this.thread_id = i;
            this.WriteblockingQueue = blockingQueue;
            this.md = table_md;
            this.prep_template = str;
            
            try
            {
                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//		Class.forName("org.apache.calcite.avatica.remote.Driver");
            }
            catch (Exception e)
            {
                LOGGER.error("Phoenix Driver Not Found");
            }
            
            try 
            {
                phoenix_connection =  DriverManager.getConnection("jdbc:phoenix:54.201.16.86:2181");
//		phoenix_connection = DriverManager.getConnection("jdbc:phoenix:thin:url=http://54.201.16.86:8765;serialization=PROTOBUF");  
                System.out.println("Connection Established : PHOENIX");
            }         
            catch (SQLException e) 
            {
                LOGGER.error("Error Establishing Connection with PHOENIX");
                e.printStackTrace();
            }
            
            
            try 
            {
                pstmt = phoenix_connection.prepareStatement(prep_template);
            } 
            catch (SQLException e) 
            {
                LOGGER.error("Unable to create Prepared Statement");
                e.printStackTrace();
            }
        }
        
        @Override
        public void run()
        {
            ArrayList<HashMap<String, Object>> chunk = null;
            
            try 
            {
                System.out.println("Taking Chunk");
                chunk = WriteblockingQueue.take();
            } 
            catch (InterruptedException e) 
            {
                LOGGER.error("Unable to receive job");
                e.printStackTrace();
            }
            
	    System.out.println("Inserting Chunk");            
            insert(chunk);
            
            try 
            {
                phoenix_connection.commit();
            } 
            
            catch (SQLException e) 
            {
                LOGGER.error("Unable to Commit the Upsert");
                e.printStackTrace();
            }
        }
        
        private void insert(ArrayList<HashMap<String,Object>> piece) 
        {
            int i = 0;
            while (i < piece.size())
            {
                try
                {                
                    for (int column_id = 1; column_id <= md.getColumnCount(); column_id++) 
                    {
                        Parameter p;
                        int coltype = md.getColumnType(column_id);
                        
                        if (column_id == 70)
                        {
                            coltype = Types.LONGNVARCHAR;
                        }

			if (column_id == 12)
			{
			   coltype = Types.CHAR;
			}
                        
                        switch (coltype)
                        {
                        case Types.VARCHAR:
                        case Types.CHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            p = PARAMETERS_MAP.get(String.class);
                            break;
                            
                        case Types.BINARY:
                            p = PARAMETERS_MAP.get(byte[].class);
                            String str = piece.get(i).get(md.getColumnName(column_id)).toString();
                            pstmt.setBytes(column_id, Bytes.toBytes(str));
                            break;
                            
                        case Types.TIMESTAMP:
                            p = PARAMETERS_MAP.get(Timestamp.class);
                            break;
                            
                        case Types.INTEGER:
                            p = PARAMETERS_MAP.get(Integer.class);
                            break;
                        case Types.BIT:
                            p = PARAMETERS_MAP.get(Boolean.class);
                            break;
                        case Types.TINYINT:
                            p = PARAMETERS_MAP.get(Integer.class);
                            break;
                        case Types.BIGINT:
                            p = PARAMETERS_MAP.get(Long.class);
                            break;
                        default:
                            LOGGER.error("Unexpected SQL Type");
                            continue;
                        }
                            
                        try
                        {                            
                            
                            if (piece.get(i).get(md.getColumnName(column_id)) == null)
                            {
                                pstmt.setNull(column_id, -1);
                            }
    
                            else
                            {
                                p.set(pstmt, column_id, piece.get(i).get(md.getColumnName(column_id)));
                            }
                        }
                        catch (Exception e)
                        {
                            pstmt.setNull(column_id, -1);
                        }
                        
                    }
                }
                catch(Exception e)
                {
                    LOGGER.error("Unable to bind parameters");
                    e.printStackTrace();
                }
                
                try 
                {
                    pstmt.addBatch();
                } 
                catch (SQLException e) 
                {
                    LOGGER.error("Unable to add Prepared Statement to the batch");
                    e.printStackTrace();
                }
                i++;
            }
            
            try 
            {
                pstmt.executeBatch();
            } 
            
            catch (SQLException e) 
            {
                LOGGER.error("Unable to execute Batch");
                e.printStackTrace();
            }
            
        }
    }

    private static interface Parameter 
    {
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException;
    }

    private static class StringParameter implements Parameter 
    {
        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setString(index, (String) value);
        }

    }

    private static class LongParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setLong(index, (Long) value);
        }

    }

    private static class IntegerParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setInt(index, (Integer) value);
        }

    }

    private static class ByteParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setByte(index, (Byte) value);
        }

    }

    private static class ByteArrayParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setBytes(index, (byte[]) value);
        }

    }

    private static class ShortParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setShort(index, (Short) value);
        }

    }

    private static class DoubleParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setDouble(index, (Double) value);
        }
    }

    private static class TimestampParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setTimestamp(index, (Timestamp)value, getUTCCalendar());
        }

    }
    
    private static Calendar getUTCCalendar() 
    {
        return Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    }

    private static class DateParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setDate(index, new java.sql.Date(((Date)value).getTime()));
        }
    }

    private static class UUIDParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setString(index, ((UUID)value).toString());
        }
    }

    private static class BooleanParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setBoolean(index, ((Boolean) value));
        }
    }
    

}

