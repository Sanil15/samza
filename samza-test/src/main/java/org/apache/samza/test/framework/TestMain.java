package org.apache.samza.test.framework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.swing.text.TableView;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.inmemory.InMemorySystemUtils.*;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.stream.CollectionStream;


public class TestMain {
  private static final String[] PAGEKEYS = {"inbox", "home", "search", "pymk", "group", "job"};

  public static void main(String args[]){

    Random random = new Random();
    int count = 10;
    ArrayList<PageView> pageviews = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String pagekey = PAGEKEYS[random.nextInt(PAGEKEYS.length - 1)];
      int memberId = random.nextInt(10);
      pageviews.add(new PageView(pagekey, memberId));
    }

    // Create a StreamTask
    StreamTask task = new StreamTask() {
      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Map<String,Object> outgoing = new HashMap<>();
        System.out.println("----------------------------------------"+envelope.getMessage()+"-----------------------------");
        outgoing.put((String) envelope.getKey(),envelope.getMessage());
        collector.send(new OutgoingMessageEnvelope(new SystemStream("test-samza","Output"), outgoing));
      }
    };
    // Create any initial config: serdes if required
    HashMap<String, String> configs = new HashMap<>();
    configs.put("serializers.registry.int.class", "org.apache.samza.serializers.IntegerSerdeFactory");
    configs.put("serializers.registry.json.class", PageViewJsonSerdeFactory.class.getName());

    // Run the test framework
    TestStreamTask.create(task, configs)
        .addInputStream(CollectionStream.of("PageView", pageviews))
        .addOutputStream(CollectionStream.empty("Output"))
        .run();
  }
}
