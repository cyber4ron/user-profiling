package com.lianjia.profiling.web.controller.prefer;

import com.alibaba.fastjson.JSON;
import com.lianjia.hdic.api.client.http.TcpProvider;
import com.lianjia.hdic.api.client.http.common.AuthUtils;
import com.lianjia.hdic.model.pojo.Bizcircle;
import com.lianjia.hdic.model.pojo.District;
import com.lianjia.hdic.model.pojo.Resblock;
import com.lianjia.hdic.model.request.GetBizcirclesReq;
import com.lianjia.hdic.model.request.GetDistrictsReq;
import com.lianjia.hdic.model.request.GetResblocksReq;
import com.lianjia.hdic.model.response.RespBase;
import com.lianjia.profiling.tagging.features.Features;
import com.lianjia.profiling.tagging.features.UserPreference;
import com.lianjia.profiling.tagging.tag.UserTag;
import com.lianjia.profiling.tagging.user.OfflineEventTagging;
import com.lianjia.profiling.tagging.user.OnlineEventTagging;
import com.lianjia.profiling.util.Properties;
import com.lianjia.profiling.web.common.AccessManager;
import com.lianjia.profiling.web.controller.kv.UserKVController;
import com.lianjia.profiling.web.dao.HBaseDao;
import com.lianjia.profiling.web.util.RespHelper;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import static com.lianjia.profiling.web.common.Constants.*;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

@RestController
public class TaggingController {
    private static final Logger LOG = LoggerFactory.getLogger(TaggingController.class.getName());
    private static final int QUERY_TIMEOUT_MS = 5000;

    private static final String TOKEN = "3ebc08668c09e7c8";
    private static final String KEY = "e525cc82fedc3a53a2b81466736d5865";
    private TcpProvider provider = TcpProvider.getInstance();

    private TransportClient client;
    private HBaseDao hbaseDao;

    private UserKVController kvCtrl;

    public TaggingController() throws UnknownHostException {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", Properties.get("es.cluster.name")).build();
        String[] nodes = Properties.get("es.cluster.nodes").split(",");

        client = TransportClient.builder().settings(settings).build();
        for (String node : nodes) {
            String[] parts = node.split(":");
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
        }

        hbaseDao = new HBaseDao();

        kvCtrl = new UserKVController();
    }

    @SuppressWarnings("Duplicates")
    private String internalGetUserOffline(String id) {
        GetRequestBuilder req = new GetRequestBuilder(client, GetAction.INSTANCE)
                .setIndex(CUST_IDX)
                .setType(CUST_TYPE)
                .setId(id);

        long startMs = System.currentTimeMillis();
        GetResponse resp = req.get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));
        System.out.println(System.currentTimeMillis() - startMs);

        return resp.getSourceAsString();
    }

    private String getBizcircleName(String bizcircleId) {
        String bizcircleName = "";
        try {
            GetBizcirclesReq req = new GetBizcirclesReq();
            req.setId(Integer.parseInt(bizcircleId));
            req.setAccessToken(TOKEN);
            req.setCurrentTimeMillis(System.currentTimeMillis());

            String md5 = AuthUtils.getSecretMd5(req, KEY);
            req.setMd5Security(md5);

            RespBase<List<Bizcircle>> resp = provider.getBizcircles(req);

            if (resp.getData().size() > 0) {
                bizcircleName = resp.getData().get(0).getName();
            }
        } catch (Exception e) {
            LOG.warn("", e);
        }

        return bizcircleName;
    }

    private String getDistrictName(String districtId) {
        String districtName = "";
        try {
            GetDistrictsReq req = new GetDistrictsReq();
            req.setId(Integer.parseInt(districtId));
            req.setAccessToken(TOKEN);
            req.setCurrentTimeMillis(System.currentTimeMillis());

            String md5 = AuthUtils.getSecretMd5(req, KEY);
            req.setMd5Security(md5);

            RespBase<List<District>> resp = provider.getDistricts(req);

            if (resp.getData().size() > 0) {
                districtName = resp.getData().get(0).getName();
            }
        } catch (Exception e) {
            LOG.warn("", e);
        }

        return districtName;
    }

    public String getResBlockName(String resBlockId) {
        String resBlockName = "";
        try {
            GetResblocksReq req = new GetResblocksReq();
            req.setId(Long.parseLong(resBlockId));
            req.setAccessToken(TOKEN);
            req.setCurrentTimeMillis(System.currentTimeMillis());

            String md5 = AuthUtils.getSecretMd5(req, KEY);
            req.setMd5Security(md5);

            RespBase<List<Resblock>> resp = provider.getResblocks(req);

            if (resp.getData().size() > 0) {
                resBlockName = resp.getData().get(0).getName();
            }
        } catch (Exception e) {
            LOG.warn("", e);
        }

        return resBlockName;
    }

    private void rewriteIdToName(UserPreference prefer) {

        // rewrite id to name
        if(prefer.getEntries().containsKey(UserTag.DISTRICT)) {
            Object[][] districts = (Object[][]) prefer.getEntries().get(UserTag.DISTRICT);
            for (int i = 0; i < districts.length; i++) {
                districts[i][0] = getDistrictName((String) districts[i][0]);
            }
        }

        if(prefer.getEntries().containsKey(UserTag.BIZCIRCLE)) {
            Object[][] bizcircles = (Object[][]) prefer.getEntries().get(UserTag.BIZCIRCLE);
            for (int i = 0; i < bizcircles.length; i++) {
                bizcircles[i][0] = getBizcircleName((String) bizcircles[i][0]);
            }
        }

        if(prefer.getEntries().containsKey(UserTag.RESBLOCK)) {
            Object[][] resblocks = (Object[][]) prefer.getEntries().get(UserTag.RESBLOCK);
            for (int i = 0; i < resblocks.length; i++) {
                resblocks[i][0] = getResBlockName((String) resblocks[i][0]);
            }
        }

    }

    @RequestMapping("/prefer/user/offline/{id}")
    @CrossOrigin
    public ResponseEntity<String> getUserOfflinePrefer(@PathVariable("id") String id,
                                                       @RequestParam(value = "token", required = false, defaultValue = "") String token) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long startMs = System.currentTimeMillis();
            String json = internalGetUserOffline(id);

            if (json == null) throw new IllegalArgumentException("id not found.");

            UserPreference prefer = OfflineEventTagging.compute(JSON.parseObject(json));

            rewriteIdToName(prefer);

            LOG.info("process time: " + (System.currentTimeMillis() - startMs));

            System.err.println(prefer);
            System.err.println(prefer.toReadableJson());

            return new ResponseEntity<>(prefer.toReadableJson(), HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalArgumentException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, ex.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


    /* ----- online, assoc uuid & ucid ----- */

    @RequestMapping("/prefer/user/online/uuid/{uuid}")
    @CrossOrigin
    public ResponseEntity<String> getUserOnlinePreferByUuid(@PathVariable("uuid") String uuid,
                                                            @RequestParam(value = "startTs", required = false, defaultValue = "") String startTsStr,
                                                            @RequestParam(value = "endTs", required = false, defaultValue = "") String endTsStr,
                                                            @RequestParam(value = "token", required = false, defaultValue = "") String token,
                                                            @RequestParam(value = "par", required = false, defaultValue = "false") String par) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long startMs = System.currentTimeMillis();

            long endTs = endTsStr.isEmpty() ? System.currentTimeMillis() : Long.parseLong(endTsStr);
            long startTs = startTsStr.isEmpty() ? new DateTime(endTs).minusDays(BACKTRACE_DAYS).getMillis() : Long.parseLong(startTsStr);

            List<Map<String, Object>> events = kvCtrl.internalGetUserOnlineByUuid(uuid, startTs, endTs, "false");

            UserPreference prefer = par.equalsIgnoreCase("true") ? computeParallel(UserTag.UUID, uuid, events) :
                    OnlineEventTagging.compute(UserTag.UUID, uuid, events);

            rewriteIdToName(prefer);

            LOG.info("query time: " + (System.currentTimeMillis() - startMs));

            LOG.info(prefer.toString());

            return new ResponseEntity<>(prefer.toReadableJson(), HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping("/prefer/user/online/ucid/{ucid}")
    @CrossOrigin
    public ResponseEntity<String> getUserOnlinePreferByUcid(@PathVariable("ucid") String ucid,
                                                            @RequestParam(value = "startTs", required = false, defaultValue = "") String startTsStr,
                                                            @RequestParam(value = "endTs", required = false, defaultValue = "") String endTsStr,
                                                            @RequestParam(value = "token", required = false, defaultValue = "") String token,
                                                            @RequestParam(value = "par", required = false, defaultValue = "false") String par) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long startMs = System.currentTimeMillis();

            long endTs = endTsStr.isEmpty() ? System.currentTimeMillis() : Long.parseLong(endTsStr);
            long startTs = startTsStr.isEmpty() ? new DateTime(endTs).minusDays(BACKTRACE_DAYS).getMillis() : Long.parseLong(startTsStr);

            List<Map<String, Object>> events = kvCtrl.internalGetUserOnlineByUcid(ucid, startTs, endTs, "false");

            UserPreference prefer = par.equalsIgnoreCase("true") ? computeParallel(UserTag.UCID, ucid, events) :
                    OnlineEventTagging.compute(UserTag.UCID, ucid, events);

            rewriteIdToName(prefer);

            LOG.info("prefer, ucid: " + ucid + ", query time: " + (System.currentTimeMillis() - startMs));

            LOG.info(ucid + ", " + prefer.toString());

            return new ResponseEntity<>(prefer.toReadableJson(), HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping("/prefer/user/online/phone/{phone}")
    @CrossOrigin
    public ResponseEntity<String> getUserOnlinePreferByPhone(@PathVariable("phone") String phone,
                                                            @RequestParam(value = "startTs", required = false, defaultValue = "") String startTsStr,
                                                            @RequestParam(value = "endTs", required = false, defaultValue = "") String endTsStr,
                                                            @RequestParam(value = "token", required = false, defaultValue = "") String token,
                                                            @RequestParam(value = "par", required = false, defaultValue = "false") String par) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long startMs = System.currentTimeMillis();

            GetRequestBuilder req = new GetRequestBuilder(client, GetAction.INSTANCE)
                    .setIndex(UCID_MAP_IDX)
                    .setId(phone);

            GetResponse resp = req.get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));
            String ucid = resp.getSourceAsMap().get("ucid").toString();

            long endTs = endTsStr.isEmpty() ? System.currentTimeMillis() : Long.parseLong(endTsStr);
            long startTs = startTsStr.isEmpty() ? new DateTime(endTs).minusDays(BACKTRACE_DAYS).getMillis() : Long.parseLong(startTsStr);

            List<Map<String, Object>> events = kvCtrl.internalGetUserOnlineByUcid(ucid, startTs, endTs, "false");

            UserPreference prefer = par.equalsIgnoreCase("true") ? computeParallel(UserTag.UCID, ucid, events) :
                    OnlineEventTagging.compute(UserTag.UCID, ucid, events);

            rewriteIdToName(prefer);

            // LOG.info("prefer, phone: " + phone + ", query time: " + (System.currentTimeMillis() - startMs));

            // LOG.info(ucid + ", " + prefer.toString());

            return new ResponseEntity<>(prefer.toReadableJson(), HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @SuppressWarnings("Duplicates")
    public static UserPreference computeParallel(UserTag idType, String id, List<Map<String, Object>> events) {

        UserPreference pf = events.stream().map(event -> { // todo: parallelStream不行
            UserPreference prefer = new UserPreference();

            if(event.containsKey("evt") && event.get("evt").equals("dtl")) {
                OnlineEventTagging.onDetail(prefer, event, Features.EventType.PC_DETAIL);
            } else if(event.containsKey("evt") && event.get("evt").equals("fl")) {
                OnlineEventTagging.onFollow(prefer, event, Features.EventType.PC_FOLLOW);
            } else if(event.containsKey("evt") && event.get("evt").equals("mob_dtl")) {
                OnlineEventTagging.onDetail(prefer, event, Features.EventType.MOBILE_DETAIL);
            } else if(event.containsKey("evt") && event.get("evt").equals("mob_fl")) {
                OnlineEventTagging.onFollow(prefer, event, Features.EventType.MOBILE_FOLLOW);
            }
            return prefer;

        }).reduce(new UserPreference(), (x, y) -> {
            x.merge(y);
            return x;
        });

        pf.updateMeta(idType, id);
        return pf;
    }
}
