package com.netflix.genie;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

@SpringBootApplication(
    exclude = {
        org.springframework.boot.autoconfigure.web.WebClientAutoConfiguration.class,
        org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class,
        org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration.class,
        org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration.class,
        net.devh.springboot.autoconfigure.grpc.server.GrpcServerAutoConfiguration.class,
        org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration.class,
        org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration.class,
        org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration.class,
        org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration.class,
        org.springframework.boot.autoconfigure.transaction.jta.JtaAutoConfiguration.class,

    }
)
public class GenieAgent {

//    @Autowired
//    StateMachine<States, Events> stateMachine;
//
//    void doSignals() {
//        stateMachine.start();
//        stateMachine.sendEvent(Events.EVENT1);
//        stateMachine.sendEvent(Events.EVENT2);
//    }


    public static void main(String[] args) {
        final long beforeInit = ManagementFactory.getRuntimeMXBean().getUptime();
        System.out.println("JVM uptime: " + beforeInit);

        final SpringApplication application = new SpringApplication(GenieAgent.class);
        application.setAddCommandLineProperties(false);
        application.run(args);

        final long afterInit = ManagementFactory.getRuntimeMXBean().getUptime();
        System.out.println("JVM uptime: " + afterInit);

        System.out.println(" >>> Spring boot configuration time: " + (afterInit - beforeInit));
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {

        return args -> {

            System.out.println("Let's inspect the beans provided by Spring Boot:");

            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                System.out.println(beanName);
            }
        };
    }

    @Bean
    public Object foo() {
        System.out.println("Args");
        for (String arg : args) {
            System.out.println("Arg: " + arg);
        }
        return null;
    }
}
