package com.xm4399.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.LinkedList;

/**
 * @Auther: czk
 * @Date: 2020/9/30
 * @Description:
 */
public class FlinkRestApiUtil {

    // cancal flink job
    public void cancalFlinkJob(String jid) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        String yarnWebIP = new ConfUtil().getValue("yarnWebIP");
        String yarnSessionID = new ConfUtil().getValue("yarnSessionAppID");
        try {
            httpClient = HttpClients.createDefault();
            String url = "http://" + yarnWebIP + "/proxy/" + yarnSessionID + "/jobs/" + jid + "/yarn-cancel";
            HttpGet httpGet = new HttpGet(url);
            httpClient.execute(httpGet, responseHandler);

        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    // 获取 flinkJob的一个信息 如state,jid等
    public String getFlinkJobOneInfo(String flinkJobName, String oneInfo) {
        String oneFlinkJobInfo = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        String yarnWebIP = new ConfUtil().getValue("yarnWebIP");
        String yarnSessionID = new ConfUtil().getValue("yarnSessionAppID");
        try {
            httpClient = HttpClients.createDefault();
            String url = "http://" + yarnWebIP + "/proxy/" + yarnSessionID + "/jobs/overview";
            //获取yarn session状态
            HttpGet httpGet = new HttpGet(url);
            String returnValue = httpClient.execute(httpGet, responseHandler); //调接口获取返回值时，必须用此方法
            JSONObject jsonObject = JSONObject.parseObject(returnValue);
            JSONArray jobs = jsonObject.getJSONArray("jobs");
            int len = jobs.size();
            for (int i = 0; i < len; i++) {
                JSONObject job = jobs.getJSONObject(i);
                if (flinkJobName.equals(job.getString("name")) && "RUNNING".equals(job.getString("state"))) {
                    oneFlinkJobInfo = job.getString(oneInfo);
                    return oneFlinkJobInfo;
                }
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return oneFlinkJobInfo;
    }

    // 获取flinkjob最后的checkpoint 路径
    public String getCheckPointPath(String flinkJobID) {
        LinkedList<String> yarnRunningJobNameList = new LinkedList<String>();
        CloseableHttpClient httpClient = HttpClients.createDefault();
        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        String yarnWebIP = new ConfUtil().getValue("yarnWebIP");
        String yarnSessionID = new ConfUtil().getValue("yarnSessionAppID");
        try {
            httpClient = HttpClients.createDefault();
            String url = "http://" + yarnWebIP + "/proxy/" + yarnSessionID + "/jobs/" + flinkJobID + "/checkpoints";
            //获取yarn session状态
            HttpGet httpGet = new HttpGet(url);
            String returnValue = httpClient.execute(httpGet, responseHandler);
            JSONObject jsonObject = JSONObject.parseObject(returnValue);
            int completedCounts = Integer.parseInt(jsonObject.getJSONObject("counts").getString("completed"));
            String checkPointPath = null;
            if (completedCounts > 0 ){
                checkPointPath = jsonObject.getJSONObject("latest").getJSONObject("completed").getString("external_path");
            }else if (completedCounts == 0){
                checkPointPath = "unfinished checkpoint, no checkpoint path ";
            }
            return checkPointPath;
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return null;
    }

}
