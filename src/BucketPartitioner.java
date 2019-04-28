import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.Random;

public class BucketPartitioner extends Partitioner <Text, IntWritable>
{
    public static HashMap <String, Integer> key_value_map = new HashMap <String, Integer> ();

    public static HashMap <String, Integer> key_partition_map = new HashMap <String, Integer> ();

    private static int number_of_partitions = 2;

    final private static int first_partition_num = 0;

    public static void estimate(String uri)
    {
        int actual_lines = 20200;
        int read_lines = 0;
        int line_count = 0;
        boolean read = false;
        try {
            Configuration config = new Configuration();

            config.set("fs.defaultFS", uri);
            config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            System.setProperty("HADOOP_USER_NAME", "cc");
            System.setProperty("hadoop.home.dir", "/");

            FileSystem fs = FileSystem.get(URI.create(uri), config);

            FSDataInputStream hdfsInStream = fs.open(new Path(uri));

            BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));

            String line;

            Random ran = new Random();

            int line_to_read = ran.nextInt(actual_lines / 100) + 1;

            while ((line = br.readLine()) != null) {

                line = line.trim();
                if (line.equals("")) continue;

                ++line_count;

                if (line_to_read != line_count) continue;

                line_to_read = ran.nextInt(actual_lines / 100) + line_to_read;

                read_lines++;

                String[] word_array = line.split("\\s+");

                for (String word : word_array)
                {
                    if (key_value_map.containsKey(word))
                    {
                        key_value_map.put(word, key_value_map.get(word) + 1);
                    }
                    else
                    {
                        key_value_map.put(word, 1);
                    }
                }
                //if (read_lines >= actual_lines / 100)
                    //break;
            }

            hdfsInStream.close();
            fs.close();

        } catch (IOException ioe) {
            // TODO Auto-generated catch block
            ioe.printStackTrace();
            System.err.println("An error occurred. Aborted");
            System.exit(1);
        }

        int sum = 0;

        key_value_map = sortByValue(key_value_map);

        for (String key : key_value_map.keySet())
        {
            sum += key_value_map.get(key);
        }

        double partition_capacity = (double)sum / number_of_partitions;

        int used_volume = 0;
        int current_partition = first_partition_num;
        for (String key : key_value_map.keySet())
        {
            int value = key_value_map.get(key);
            if (used_volume + value <= partition_capacity * (current_partition + 1))
            {
                key_partition_map.put(key, current_partition);
            }
            else
            {
                key_partition_map.put(key, current_partition + 1);
                current_partition++;
            }
            used_volume += value;
        }
    }

    protected static HashMap <String, Integer> sortByValue(HashMap <String, Integer> hm)
    {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer> > list =
                new LinkedList<Map.Entry<String, Integer> >(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2)
            {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        // put data from sorted list to hashmap
        HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

    @Override
    public int getPartition(Text key, IntWritable value, int num_partitions) // Make every partition roughly equal
    {
        String key_string = key.toString();
        if (key_partition_map.containsKey(key_string))
        {
            int part_num = key_partition_map.get(key_string);
            if (part_num >= 0 && part_num < num_partitions)
            {
                return part_num;
            }
            return 0;
        }
        return 0; // Error : No suitable partition to allocate.
    }

    public static void main(String[] args) // For testing
    {
        String core_site = "hdfs://localhost:19000";
        estimate(core_site + "/user/hadoop/input/test.txt");
        //estimate("file:///D://words.txt");
        System.out.println(key_partition_map);
    }
}
