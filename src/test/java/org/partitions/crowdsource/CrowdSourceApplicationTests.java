package org.partitions.crowdsource;

import org.hibernate.annotations.Check;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.partitions.crowdsource.service.NativePartitionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CrowdSourceApplicationTests {

  @Autowired
  CheckinsRepository mCheckinsRepository;
  @Autowired
  NativePartitionService mNativePartitionService;

	@Test
	public void contextLoads() {
	}

	@Test
  public void testCheckinsRepository(){
    Iterable<Checkins> checkins = mCheckinsRepository.findAll();
    List<Checkins> checkinsList = new ArrayList<>();
    Iterable<Checkins> checkinsGroup = mCheckinsRepository.findAllGroupByDate();
    checkins.forEach(checkinsList::add);
    Assert.assertNotNull(mCheckinsRepository.findAllGroupByDate());
  }

  @Test
  public void testDateObjectConversionService()throws Exception{
	  List<Checkins> checkinsList = CrowdSourceUtils.convertFromIterableToList(mCheckinsRepository.findAll());
	  Assert.assertEquals(checkinsList.get(0).getDateObj().getClass(), Date.class);
  }


  @Test
  public void testGroupBy() {
    Iterable<Checkins> checkins = mCheckinsRepository.findAll();
    List<Checkins> checkinsList = CrowdSourceUtils.convertFromIterableToList(checkins);
    Map<Date, List<Checkins>> listMap = mNativePartitionService.getGroupBy(checkinsList);
    Assert.assertNotNull(listMap);
  }

  @Test
  public void testAssignPartitions(){
    Iterable<Checkins> checkins = mCheckinsRepository.findAll();
    List<Checkins> checkinsList = CrowdSourceUtils.convertFromIterableToList(checkins);
    checkinsList.sort((o1,o2)->o1.getDate().compareTo(o2.getDate()));
    //Map<Date, List<Checkins>> listMap = mNativePartitionService.getGroupBy(checkinsList);

    mNativePartitionService.assignPartitions(checkinsList.subList(0,5000));

  }


}
