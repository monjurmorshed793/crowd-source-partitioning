package org.partitions.crowdsource.service;

import io.reactivex.Observable;
import io.reactivex.internal.schedulers.ComputationScheduler;
import io.reactivex.schedulers.Schedulers;
import org.hibernate.annotations.Check;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleGraph;
import org.partitions.crowdsource.Checkins;
import org.partitions.crowdsource.CheckinsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.IntStream;

/**
 * Created by Monjur-E-Morshed on 12-Feb-18.
 */
@Service
public class NativePartitionService {
  @Autowired
  CheckinsRepository mCheckinsRepository;

  public List<Checkins> getListWithDateObject(List<Checkins> pCheckins){
    Observable<Checkins> observable = Observable.fromIterable(pCheckins);
    observable
        .subscribeOn(Schedulers.computation())
        .subscribe(
            (i)->{
              i.setDateObj(new SimpleDateFormat("yyyy-MM-dd").parse(i.getDate()));
            }
        );
    return pCheckins;
  }

  public Map<Date, List<Checkins>> getGroupBy(List<Checkins> pCheckins){
    Map<Date, List<Checkins>> groupdedCheckins = new HashMap<>();


    Observable.fromIterable(pCheckins)
        .subscribe(v->{
          List<Checkins> checkinsList = new ArrayList<>();
          if(groupdedCheckins.containsKey(v.getDateObj())){
            checkinsList = groupdedCheckins.get(v.getDateObj());
            checkinsList.add(v);
            groupdedCheckins.put(v.getDateObj(), checkinsList);
          }else{
            checkinsList.add(v);
            groupdedCheckins.put(v.getDateObj(), checkinsList);
          }
        });
    return groupdedCheckins;
  }

  public Integer getThresholdValue(Integer pThresholdValue){
    return pThresholdValue==null?3000:pThresholdValue;
  }


  public Graph<Integer, DefaultWeightedEdge> createGraph(List<Checkins> pCheckins){
    Graph<Integer, DefaultWeightedEdge> graph = new SimpleGraph<>(DefaultWeightedEdge.class);
    return null;
  }


  public void assignPartitions( List<Checkins> pCheckinsList){
    int partitionNumber=1;
    Integer pThresholdValue=3000;
    int counter=1;
    List<Checkins> updatedList = new ArrayList<>();
    while(pCheckinsList.size()!=0){
      if(counter>500){
        counter=1;
        partitionNumber+=1;
      }
      Checkins deletedCheckin = pCheckinsList.remove(0);
      deletedCheckin.setNaiveLal(partitionNumber);
      updatedList.add(deletedCheckin);
      Map<Double, Checkins> mapDistance = new HashMap<>();
      List<Double> distanceList = new ArrayList<>();
      for(int i=0; i<pCheckinsList.size();i++){
        Checkins checkin = pCheckinsList.get(i);
        double distance = distance(deletedCheckin.getLatitude(), deletedCheckin.getlongitude(), checkin.getLatitude(), checkin.getlongitude(),'N');
        mapDistance.put(distance, checkin);
        distanceList.add(distance);
      }
      Collections.sort(distanceList);
      if(distanceList.size()==0) break;
      Checkins memberToBeDeleted=mapDistance.get(distanceList.get(0));
      memberToBeDeleted.setNaiveLal(partitionNumber);
      pCheckinsList.remove(memberToBeDeleted);
      counter+=2;
      updatedList.add(memberToBeDeleted);
    }
    mCheckinsRepository.save(updatedList);
  }


  private double distance(double lat1, double lon1, double lat2, double lon2, char unit) {
    double theta = lon1 - lon2;
    double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
    dist = Math.acos(dist);
    dist = rad2deg(dist);
    dist = dist * 60 * 1.1515;
    if (unit == 'K') {
      dist = dist * 1.609344;
    } else if (unit == 'N') {
      dist = dist * 0.8684;
    }
    return (dist);
  }

  private double deg2rad(double deg) {
    return (deg * Math.PI / 180.0);
  }

  /*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
  /*::  This function converts radians to decimal degrees             :*/
  /*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
  private double rad2deg(double rad) {
    return (rad * 180.0 / Math.PI);
  }

}
