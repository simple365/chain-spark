package com.tom.spark;

import java.text.SimpleDateFormat;
import java.util.*;

public class ChainJson {



    public static String getDay(String timestamp) {
        String timeStr = timestamp.substring(0, timestamp.length()-3);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd");
        long lt = Long.valueOf(timeStr);
        Date dateTime = new Date(lt);
        return simpleDateFormat.format(dateTime);
    }
}
