package com.atguigu.gmall.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ author: Wanger
 * @ time: 2020/11/14 10:47
 * @ desc:
 */

@RestController
public class PublisherController {
	@Autowired
	PublisherService publisherService;

	// http://localhost:8070/realtime-total?date=2020-03-31
	@GetMapping("/realtime-total")
	public String getRealtimeTotal(@RequestParam("date") String date) {
		List<Map> totalList = new ArrayList<>();
		Map<String, String> dauMap = new HashMap<>();
		dauMap.put("id", "dau");
		dauMap.put("name", "新增日活");
		dauMap.put("value", publisherService.getDauTotal(date).toString());
		totalList.add(dauMap);

		Map midMap = new HashMap<>();
		midMap.put("id", "new_mid");
		midMap.put("name", "新增设备");
		midMap.put("value", "1000");
		totalList.add(midMap);

		return JSON.toJSONString(totalList);
	}

	// http://localhost:8070/realtime-hour?id=dau&date=2020-03-31
	@GetMapping("/realtime-hour")
	public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
		if ("dau".equals(id)) {
			Map todayDauHourMap = publisherService.getDauHour(date);
			Map yesterdayDauHourMap = publisherService.getDauHour(LocalDate.parse(date).plusDays(-1).toString());

			Map<String, Map> hourMap = new HashMap<>();
			hourMap.put("today", todayDauHourMap);
			hourMap.put("yesterday", yesterdayDauHourMap);
			return JSON.toJSONString(hourMap);
		}
		return null;
	}
}