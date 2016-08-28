package com.lianjia.profiling.web.controller.thridparty;

import com.lianjia.hdic.api.client.http.TcpProvider;
import com.lianjia.hdic.api.client.http.common.AuthUtils;
import com.lianjia.hdic.model.pojo.FullBuilding;
import com.lianjia.hdic.model.pojo.FullHouse;
import com.lianjia.hdic.model.request.GetFullBuildingsReq;
import com.lianjia.hdic.model.request.GetFullHousesReq;
import com.lianjia.hdic.model.response.RespBase;
import com.lianjia.profiling.web.common.AccessManager;
import com.lianjia.profiling.web.domain.Request;
import com.lianjia.profiling.web.util.RespHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author fenglei@lianjia.com on 2016-08
 */

@RestController
public class HdicController {
    private static final Logger LOG = LoggerFactory.getLogger(HdicController.class.getName());

    private static final String TOKEN = "3ebc08668c09e7c8";
    private static final String KEY = "e525cc82fedc3a53a2b81466736d5865";
    private TcpProvider provider = TcpProvider.getInstance();

    private Map<String, String> getLocation(List<Long> hdicHouseIds) {
        Map<String, String> locations = new HashMap<>();
        try {
            locations = hdicHouseIds.stream().collect(Collectors.toMap(Object::toString, houseId -> {
                // get building id
                GetFullHousesReq req = new GetFullHousesReq();
                req.setId(houseId);
                req.setAccessToken(TOKEN);
                req.setCurrentTimeMillis(System.currentTimeMillis());
                req.setMd5Security(AuthUtils.getSecretMd5(req, KEY));

                RespBase<List<FullHouse>> resp = provider.getFullHouses(req);
                if (resp.getData().isEmpty()) return "";

                Long buildingId = resp.getData().get(0).getBuildingId();

                // get building location
                GetFullBuildingsReq reqBuilding = new GetFullBuildingsReq();
                reqBuilding.setId(buildingId);
                reqBuilding.setAccessToken(TOKEN);
                reqBuilding.setCurrentTimeMillis(System.currentTimeMillis());
                reqBuilding.setMd5Security(AuthUtils.getSecretMd5(reqBuilding, KEY));

                RespBase<List<FullBuilding>> respBuilding = provider.getFullBuildings(reqBuilding);
                if (resp.getData().isEmpty()) return "";

                return respBuilding.getData().get(0).getPointLng().toString() +
                        "," + respBuilding.getData().get(0).getPointLat().toString();

            })).entrySet().stream().filter(x -> !x.getValue().isEmpty())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        } catch (Exception e) {
            LOG.warn("", e);
        }

        return locations;
    }


    @RequestMapping(path = "/hdic/house/location", method = RequestMethod.POST)
    @CrossOrigin
    public ResponseEntity<String> getCustomerOffline(@RequestBody Request.BatchKVLongRequest request) {
        try {
            if (!AccessManager.checkKV(request.token)) throw new IllegalAccessException();
            if (request.ids.size() > 100) throw new IllegalArgumentException("too many ids");

            Map<String, String> locations = getLocation(request.ids);

            return new ResponseEntity<>(RespHelper.getSuccResp(locations),
                                        HttpStatus.OK);
        } catch (Exception ex) {
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, ex.getMessage()),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
