package com.essexboy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    final static Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        try {
            RebalanceService rebalanceService = new RebalanceService();
            rebalanceService.rebalance();
        } catch (Exception e) {
            LOGGER.error("error", e);
            help();
        }
    }

    private static void help() {
        System.out.println("kafka-rebalance-0.1.jar rebalance-config.json [dryrun=false]");
        System.exit(0);
    }
}
