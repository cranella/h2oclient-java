package com.att.h2o;

import okhttp3.ResponseBody;
import water.bindings.pojos.ColSpecifierV3;
import water.bindings.pojos.FrameKeyV3;
import water.bindings.pojos.KeyV3;

import java.io.*;
import java.lang.reflect.Field;

public class Util {

    public static void copyFields(Object to, Object from) {
        Field[] fromFields = from.getClass().getDeclaredFields();

        for(Field fromField: fromFields) {

            try {
                Field toField = to.getClass().getDeclaredField(fromField.getName());
                fromField.setAccessible(true);
                toField.setAccessible(true);
                toField.set(to, fromField.get(from));
            } catch (Exception e) {
                ;
            }
        }
    }

    public static String keyToString(KeyV3 key) {
        return key == null ? null : key.name;
    }

    public static FrameKeyV3 stringToFrameKey(String key) {
        if (key == null) {
            return null;
        } else {
            FrameKeyV3 k = new FrameKeyV3();
            k.name = key;
            return k;
        }
    }

    public static String[] keyArrayToStringArray(KeyV3[] keys) {
        if (keys == null) {
            return null;
        } else {
            String[] ids = new String[keys.length];

            for(int i=0; i < keys.length; i++) {
                ids[i] = keys[i].name;
            }

            return ids;
        }
    }

    public static String colToString(ColSpecifierV3 col) {
        return col == null ? null : col.columnName;
    }

    public static ColSpecifierV3 stringToCol(String col) {
        if (col == null) {
            return null;
        } else {
            ColSpecifierV3 c = new ColSpecifierV3();
            c.columnName = col;
            return c;
        }
    }

    public static void writeFile(ResponseBody body, File file, FileDownloadListener listener) {

        try(InputStream inputStream = body.byteStream();
            OutputStream outputStream = new FileOutputStream(file,false)) {

            byte[] buffer = new byte[4096];

            long fileSize = body.contentLength();
            long fileSizeDownloaded = 0;
            int bytesRead;

            while((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
                fileSizeDownloaded += bytesRead;
                listener.setStatus(new FileDownload(file.getPath(), fileSize, fileSizeDownloaded, false));
            }

            outputStream.flush();
            listener.setStatus(new FileDownload(file.getPath(),fileSize,fileSizeDownloaded, true));

        } catch (IOException e) {
            listener.setError(e.toString());
        }
    }

}
