# 智慧交通车流量监控项目

### 数据来源

monitor_flow_action 监控数据表

~~~scala
data 日期
monitor_id 卡扣号
camera_id 摄像头编号
car 车牌
action_time 某个摄像头拍摄时间 s
speed 通过卡扣的速度
road_id 道路id
area_id 区域id
~~~

monitor_camera_info 卡扣与摄像头基本关系表

~~~scala
monitor_id 卡扣编号
camera_id 摄像头编号
~~~



### 需求说明

1. 卡扣监控

   全部使用SparkCore实现

   * 筛选条件内的卡扣信息

     根据使用者传入的指定条件，筛选出指定的一批卡扣信息

   * 检测卡扣状态

     基于第一步查询出来的卡扣信息结果来实现

     检查卡扣（monitor_id）状态，卡扣状态分为正常，异常状态。

     摄像头（camera_id）的状态分为正常，异常。

     异常摄像头的所有信息（monitor_id: camera_id）

2. 通过车辆数最多的Top N卡扣

3. 统计Top N 卡扣下经过的所有车辆详细信息

4. 车辆通过速度相对比较快的Top N卡扣

5. 卡扣 ‘‘0001’’下所有的车辆轨迹

   * 过滤日期范围内 卡扣“0001”下有那些车辆
   * 过滤日期范围内 这些车辆经过卡扣的时间，按照时间升序排序

6. 车辆碰撞

7. 随机抽取车辆

8. 卡扣流量转换率

9. 实时道路拥堵情况

10. 动态改变广播变量

11. 统计每个区域中车辆最多的前3道路

    

