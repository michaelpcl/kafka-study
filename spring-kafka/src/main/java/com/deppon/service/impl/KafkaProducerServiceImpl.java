package com.deppon.service.impl;

import com.deppon.service.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
public class KafkaProducerServiceImpl implements KafkaProducerService {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerServiceImpl.class);
	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;
	
	public void sendDefaultInfo(String str) {
		logger.info("----message--send----");
		kafkaTemplate.sendDefault(str);
	}

}
