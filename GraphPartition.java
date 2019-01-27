import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    
    public Vertex() {
        
    }

    Vertex(long id, Vector<Long> adjacent,long centroid,short depth) {
        
        this.centroid = centroid;
        this.depth=depth;
        this.id = id;
        this.adjacent = adjacent;
    }

    Vertex( short depth,long centroid) {
        this.depth = depth;
        this.centroid = centroid;
        this.id = 0;
        this.adjacent = new Vector<Long>();
    }

    
    public long get_id() {
        return id;
    }

    public void set_id(long id) {
        this.id = id;
    }

    public Vector<Long> getAdj() {
        return adjacent;
    }

    public void setAdj(Vector<Long> adjacent) {
        this.adjacent = adjacent;
    }

    public long getcentroid() {
        return centroid;
    }

    public void setcentroid(long centroid) {
        this.centroid = centroid;
    }

    public short getdepth() {
        return depth;
    }

    public void setdepth(short depth) {
        this.depth = depth;
    }

    
    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        
        int size = in.readInt();
        adjacent = new Vector<Long>();
        depth = in.readShort();
        centroid = in.readLong();
        id = in.readLong();
        for (int i = 0; i < size; i++) {
            long indi_size = in.readLong();
            adjacent.add(indi_size);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub

        out.writeInt(adjacent.size());
        out.writeLong(centroid);
        out.writeLong(id);
        for (long val : adjacent) {
            out.writeLong(val);
        }
    }

        public String toString() {
        return  id + " " + adjacent.toString()+" "+centroid +" "+depth;
    }
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;

    public static class MapperI extends Mapper<Object, Text, LongWritable, Vertex> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] arr = str.split(",");
            Vector<Long> Adj = new Vector<Long>();
            Vertex ver = new Vertex();
            for (int i = 0; i < arr.length; i++) {
                Adj.add(Long.parseLong(arr[i]));
            }
             

            try{ 
            for (int i = 0; i < arr.length; i++) 
            {
                ver = new Vertex(Long.parseLong(arr[i]), Adj,Long.parseLong(arr[i]), (short)0);
                context.write(new LongWritable(Long.parseLong(arr[i])), ver);           
            }

            }             
            catch(Exception e)
            {
            System.out.println(e);
            }
            
        
        }
    }

    public static class MapperII extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
        public void map(LongWritable key, Vertex val, Context context) throws IOException, InterruptedException {
            
            context.write(new LongWritable(val.get_id()), val);
            if (val.getcentroid() > 0)
            {
            for (long n : val.getAdj()) 
                {
                
                context.write(new LongWritable(n), new Vertex(n,null,val.getcentroid(),BFS_depth));
                
                }
            }
        }
    }

    public static class ReducerI extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
        public void reduce(LongWritable key, Iterable<Vertex> val, Context context) throws IOException, InterruptedException, NullPointerException{
            short min_depth = 1000;
            long x=key.get();
            Vertex m = new Vertex(x,null,(long)-1,(short)0);

            for (Vertex v : val) 
            {
                if (v.getAdj() != null) {
                    m.setAdj(v.getAdj()) ;
                }
                if (v.getcentroid()>0 && v.getdepth()<min_depth){
                    min_depth=v.getdepth();
                    m.setcentroid(v.getcentroid());
                }
                
            }
            m.depth = min_depth;
            
            context.write(new LongWritable(m.get_id()), m);
        }
    }

    public static class MapperIII extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
        

        public void map(LongWritable key, Vertex val, Context context) throws IOException, InterruptedException {
        long z=(long)1;
            context.write(new LongWritable(val.getcentroid()), new LongWritable(z));
        }
    }

    public static class ReducerII extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        public void reduce(LongWritable key, Iterable<LongWritable> val, Context context)throws IOException, InterruptedException {
            long m = 0;
            for (LongWritable value : val) {
                m = m + value.get();
            }
            context.write(key, new LongWritable(m));
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(GraphPartition.class);

        job.setMapperClass(MapperI.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]+"/i0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < 8; i++ ) {
            BFS_depth++;
            Job job1 = Job.getInstance();
            job1.setJobName("Second Map-Reduce Step");
            job1.setJarByClass(GraphPartition.class);

            job1.setMapperClass(MapperII.class);
            job1.setReducerClass(ReducerI.class);
            
            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(Vertex.class);
            
            job1.setOutputKeyClass(LongWritable.class);
            job1.setOutputValueClass(Vertex.class);
            
            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(SequenceFileOutputFormat.class);
            
            SequenceFileInputFormat.setInputPaths(job1, new Path(args[1]+"/i"+i));
            SequenceFileOutputFormat.setOutputPath(job1, new Path(args[1]+"/i"+(i+1)));
            job1.waitForCompletion(true);
        }
        Job job2 = Job.getInstance();
        job2.setJobName("Final Step");
        job2.setJarByClass(GraphPartition.class);
        
        job2.setMapperClass(MapperIII.class);
        job2.setReducerClass(ReducerII.class);
        
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        SequenceFileInputFormat.setInputPaths(job2, new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));job.waitForCompletion(true);
        job2.waitForCompletion(true);
    }
}
