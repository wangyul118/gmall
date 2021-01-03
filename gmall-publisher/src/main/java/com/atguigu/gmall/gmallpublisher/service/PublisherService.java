package com.atguigu.gmall.gmallpublisher.service;

import java.util.Map;

/**
 * @ author: Wanger
 * @ time: 2020/11/14 10:23
 * @ desc:
 */
public interface PublisherService {
	//查询总数
	Long getDauTotal(String date);

	//查询小时明细
	Map<String,Long> getDauHour(String date);
}
