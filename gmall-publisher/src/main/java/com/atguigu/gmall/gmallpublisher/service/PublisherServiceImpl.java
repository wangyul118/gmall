package com.atguigu.gmall.gmallpublisher.service;

import com.atguigu.gmall.gmallpublisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ author: Wanger
 * @ time: 2020/11/14 10:27
 * @ desc:
 */
@Service
class PublisherServiceImpl  implements  PublisherService{

	//自动注入DauMapper对象
	@Autowired
	DauMapper dauMapper;


	@Override
	public Long getDauTotal(String date) {

		return dauMapper.getDauTotal(date);
	}

	@Override
	public Map<String,Long> getDauHour(String date) {
		List<Map> dauHourList = dauMapper.getDauHour(date);
		Map dauHourMap = new HashMap();
		for (Map map : dauHourList) {
			String hour = (String) map.get("HOUR");
			Long count = (Long) map.get("COUNT");
			dauHourMap.put(hour,count);
		}
		return dauHourMap;
	}
}