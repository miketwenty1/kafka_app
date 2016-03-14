package com.test.groups;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.json.*;
import java.sql.Timestamp;
import java.util.*;


/**
 * Created by tidwell on 3/12/16.
 */
public class kafka_new {

    public static void main(String[] args) throws JSONException {

        HaversineDistance empDist = new HaversineDistance();
        TopicPartition topicpart0 = new TopicPartition("small", 0);
        TopicPartition topicpart1 = new TopicPartition("small", 1);
        TopicPartition topicpart2 = new TopicPartition("small", 2);
        TopicPartition topicpart3 = new TopicPartition("small", 3);
        ArrayList<String> suspects = new ArrayList<>();
        List<TopicPartition> topiclist = new ArrayList<>();
        topiclist.add(topicpart0);
        topiclist.add(topicpart1);
        topiclist.add(topicpart2);
        topiclist.add(topicpart3);
        HashMap<String, ArrayList<String>> employeeLogin = new HashMap<>();

        Properties props = new Properties();
        //props.put("bootstrap.servers", "104.197.94.187:6667,104.197.212.40:6667,104.197.224.31:6667");
        props.put("bootstrap.servers", "104.154.53.184:6667, 104.154.39.115:6667, 104.154.49.129:6667");
        props.put("group.id", "testererx2112");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.assign(topiclist);
        consumer.seekToBeginning(topicpart0,topicpart1,topicpart2,topicpart3);
        int i = 0;
        while (i<2500) {

            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                i++;
                //System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                JSONObject obj = new JSONObject(record.value());
                String employeeId = obj.getString("employeeId");
                String lat = obj.getString("lat");
                String lon = obj.getString("long");
                String time = obj.getString("time");
                String id = obj.getString("id");


                ArrayList<String> loginInfo = new ArrayList<>();
                loginInfo.add(lat);
                loginInfo.add(lon);
                time = time.replace("T"," ");
                time = time.replace("Z","");
                Timestamp ts = Timestamp.valueOf(time);
                double numTime = ts.getTime();
                loginInfo.add(String.valueOf(numTime));

                if (employeeLogin.containsKey(employeeId))
                {

                    double lat_old = Double.parseDouble(employeeLogin.get(employeeId).get(0));
                    double lon_old = Double.parseDouble(employeeLogin.get(employeeId).get(1));
                    double lat_new = Double.parseDouble(lat);
                    double lon_new = Double.parseDouble(lon);

                    double diffDistance = empDist.haversine(lat_old, lon_old, lat_new, lon_new);
                    //System.out.println(realDiffDistance);
                    double t1 = Double.parseDouble(employeeLogin.get(employeeId).get(2));
                    Boolean good = empDist.compute(diffDistance, t1, numTime);
                    if (good)
                    {
                        employeeLogin.put(employeeId,loginInfo);
                    }
                    else
                    {
                        suspects.add(employeeId);
                        System.out.println(id);
                    }

                }
                else
                {
                    //System.out.println("new employee added: "+employeeId);
                    employeeLogin.put(employeeId,loginInfo);
                }
                System.out.println(i);
            }

        }
    }
}
