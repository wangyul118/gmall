package com.atguigu.gmall.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @ author: Wanger
 * @ time: 2020/11/14 9:57
 * @ desc: 数据库查询数据的接口
 */
public interface DauMapper {
	//获取日活的接口
	Long getDauTotal(String date);

	//查询小时明细
	List<Map> getDauHour(String date);

}
