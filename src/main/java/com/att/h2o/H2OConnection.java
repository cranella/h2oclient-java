package com.att.h2o;

import com.att.h2o.proxies.retrofit.PostFile;
import com.att.h2o.rapids.RapidsSchemaDeserializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import retrofit2.Call;
import retrofit2.Converter;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
import water.bindings.pojos.*;
import water.bindings.proxies.retrofit.*;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates a connection to a specific H2O cluster as a singleton object. All operations performed on the cluster
 * are sent as requests within the context of an <code>H2OConnection</code> instance. These include all cluster operations
 * performed by methods in <code>H2OFrame</code>, <code>H2OModel</code>, <code>H2OModelBuilder</code>, and derived subclasses.
 * This class also serves as an entry point to your cluster for uploading/importing data and running parse jobs to convert raw data to frames.
 * <p>
 * Rapids expression evaluation involves obtaining and passing a session key with each request which is stored in this class
 * and automatically generated upon the first call to evaluate any Rapids expression, or in other words, when using the methods in <code>H2OFrame</code>
 * to manipulate frame data.
 * <p>
 * Call <code>H2OConnection.newInstance(Http url)</code> to initialize.  To close an opened Rapids session, call <code>H2OConnection.close()</code>
 * or initialize in a try-with-resources block as this class implements interface <code>AutoCloseable</code>.
 *
 *@author Chris Ranella
 *
 *@see H2OFrame
 *@see H2OModel
 *@see H2OModelBuilder
 *
 */
public class H2OConnection implements AutoCloseable {

    private Retrofit retrofit;
    private HashMap<Class<?>,Object> retrofitServices = new HashMap<>();

    private String sessionKey;

    private static int maxRetries = 3;
    private static int timeoutSec = 30;
    private static int pollIntervalMillis = 1000;
    private static int retryIntervalMillis = 200;
    private static HttpLoggingInterceptor.Level httpLogLevel = HttpLoggingInterceptor.Level.NONE;
    private static HttpLoggingInterceptor.Logger httpLogger = HttpLoggingInterceptor.Logger.DEFAULT;
    private static HttpUrl url;
    private static H2OConnection instance;

    public static H2OConnection newInstance(HttpUrl url) throws H2OException {
        Gson gson = (new GsonBuilder())
                .registerTypeAdapterFactory(new H2OProvidedTypeAdapters.ModelV3TypeAdapter())
                .registerTypeAdapter(KeyV3.class, new H2OProvidedTypeAdapters.KeySerializer())
                .registerTypeAdapter(ColSpecifierV3.class, new H2OProvidedTypeAdapters.ColSerializer())
                .registerTypeAdapter(ModelBuilderSchema.class, new H2OProvidedTypeAdapters.ModelDeserializer())
                .registerTypeAdapter(ModelSchemaBaseV3.class, new H2OProvidedTypeAdapters.ModelSchemaDeserializer())
                .registerTypeAdapter(ModelOutputSchemaV3.class, new H2OProvidedTypeAdapters.ModelOutputDeserializer())
                .registerTypeAdapter(ModelParametersSchemaV3.class, new H2OProvidedTypeAdapters.ModelParametersDeserializer())
                .registerTypeAdapter(FrameV4.class, new FrameDeserializer())
                .registerTypeAdapter(RapidsSchemaV3.class, new RapidsSchemaDeserializer())
                .registerTypeAdapter(H2OErrorV3.class, new H2OErrorDeserializer())
                .create();
        HttpLoggingInterceptor loggingInterceptor = new HttpLoggingInterceptor(httpLogger);
        loggingInterceptor.setLevel(httpLogLevel);
        OkHttpClient client = (new OkHttpClient.Builder()).connectTimeout((long)timeoutSec, TimeUnit.SECONDS)
                .writeTimeout((long)timeoutSec, TimeUnit.SECONDS)
                .readTimeout((long)timeoutSec, TimeUnit.SECONDS)
                .addInterceptor(loggingInterceptor).build();
        Retrofit retrofit = (new Retrofit.Builder())
                .client(client)
                .baseUrl(url)
                .addConverterFactory(GsonConverterFactory.create(gson))
                .build();
        if (H2OConnection.url != url) {
            if(!validateConnection(retrofit)) {
                throw new H2OException("Could not initialize H2O connection.  URL: " + retrofit.baseUrl());
            }
            H2OConnection.url = url;
        }
        if(instance != null) {
            instance.close();
        }
        instance = new H2OConnection(retrofit);
        return instance;
    }

    public static H2OConnection getInstance() {
        if(instance == null) {
            throw new IllegalStateException("No H2OConnection instance initialized. Call H2OConnection.newInstance(HttpUrl)");
        }
        return instance;
    }

    private H2OConnection(Retrofit retrofit) {
        this.retrofit = retrofit;
    }

    public static void setHttpLogLevel(HttpLoggingInterceptor.Level logLevel) {
        httpLogLevel = logLevel;
        if(instance != null) {
            try {
                newInstance(H2OConnection.url);
            } catch (H2OException e) {
                e.printStackTrace();
            }
        }
    }

    public static void setHttpLogger(HttpLoggingInterceptor.Logger logger) {
        httpLogger = logger;
        if(instance != null) {
            try {
                newInstance(H2OConnection.url);
            } catch (H2OException e) {
                e.printStackTrace();
            }
        }
    }

    public static void setTimeout(int s) {
        timeoutSec = s;
        if(instance != null) {
            try {
                newInstance(H2OConnection.url);
            } catch (H2OException e) {
                e.printStackTrace();
            }
        }
    }

    public static void setMaxRetries(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("max retries < 0");
        }
        maxRetries = n;
    }

    public static void setPollInterval(int ms) {
        if (ms < 0) {
            throw new IllegalArgumentException("poll interval < 0");
        }
        pollIntervalMillis = ms;
    }

    public static void setRetryInterval(int ms) {
        if (ms < 0) {
            throw new IllegalArgumentException("retry interval < 0");
        }
        retryIntervalMillis = ms;
    }

    public CloudV3 getStatus() throws H2OException {
        return executeWithRetries(getService(Cloud.class).status());
    }

    public boolean isValid() {
        try {
            Response<CloudV3> response = getService(Cloud.class).status().execute();
            return response.headers().get("X-h2o-cluster-good").equals("true");
        } catch (IOException e) {
            return false;
        }
    }

    public JobV3 poll(String jobId) throws H2OException {
        JobsV3 jobs = executeWithRetries(getService(Jobs.class).fetch(jobId));
        if (jobs.jobs == null || jobs.jobs.length != 1) {
            throw new H2OException("Failed to find job: " + jobId);
        }
        return jobs.jobs[0];
    }

    public void cancelJob(String jobId) throws H2OException {
        executeWithRetries(getService(Jobs.class).cancel(jobId, null));
    }

    public JobV3 waitForJobCompletion(String jobId) throws H2OException {
        JobsV3 jobs;
        do {
            try {
                Thread.sleep(pollIntervalMillis);
            } catch (InterruptedException e) {
                //No interruptions expected
            }
            jobs = executeWithRetries(getService(Jobs.class).fetch(jobId));
            if (jobs.jobs == null || jobs.jobs.length != 1) {
                throw new H2OException("Failed to find job: " + jobId);
            }

        } while(jobs.jobs[0].status.equals("RUNNING"));

        return jobs.jobs[0];
    }

    public H2OFrame uploadFileToFrame(File file) throws H2OException {
        String[] source = {upload(file, false)};
        ParseV3 parseParams = parseSetup(source);
        parseParams.deleteOnDone = true;
        return parse(parseParams);
    }

    public H2OFrame uploadFileToFrame(File file, String destinationFrame) throws H2OException {
        String[] source = {upload(file, false)};
        ParseV3 parseParams = parseSetup(source);
        parseParams.destinationFrame = Util.stringToFrameKey(destinationFrame);
        parseParams.deleteOnDone = true;
        return parse(parseParams);
    }

    public H2OFrame uploadFileToFrame(File file, ParseV3 parseParams) throws H2OException {
        String source = upload(file, false);
        parseParams.sourceFrames = new FrameKeyV3[]{Util.stringToFrameKey(source)};
        return parse(parseParams);
    }

    public H2OFrame importFileToFrame(String path) throws H2OException {
        String[] source = {importRemoteFile(path)};
        ParseV3 parseParams = parseSetup(source);
        parseParams.deleteOnDone = true;
        return parse(parseParams);
    }

    public H2OFrame importFileToFrame(String path, String destinationFrame) throws H2OException {
        String[] source = {importRemoteFile(path)};
        ParseV3 parseParams = parseSetup(source);
        parseParams.destinationFrame = Util.stringToFrameKey(destinationFrame);
        parseParams.deleteOnDone = true;
        return parse(parseParams);
    }

    public H2OFrame importFileToFrame(String path, ParseV3 parseParams) throws H2OException {
        String source = importRemoteFile(path);
        parseParams.sourceFrames = new FrameKeyV3[]{Util.stringToFrameKey(source)};
        return parse(parseParams);
    }

    public String importRemoteFile(String path) throws H2OException {
        if(path == null) {
            throw new NullPointerException();
        }
        ImportFilesV3 responseBody = executeWithRetries(getService(ImportFiles.class).importFiles(path));
        if (responseBody.destinationFrames == null || responseBody.destinationFrames.length != 1) {
            throw new H2OException("File import failed. Path: " + path);
        }
        return responseBody.destinationFrames[0];
    }

    public String[] importRemoteFiles(String path, String pattern) throws H2OException {
        if(path == null || pattern == null) {
            throw new NullPointerException();
        }
        ImportFilesV3 responseBody = executeWithRetries(getService(ImportFiles.class).importFiles(path, pattern, null));
        if (responseBody.destinationFrames == null || responseBody.destinationFrames.length < 1) {
            throw new H2OException("File imports failed. Path: " + path +", Pattern: " + pattern);
        }
        return responseBody.destinationFrames;
    }

    public String upload(File file, boolean showProgress) throws H2OException {
        //make sure timeout is not an issue
        if (file == null ) {
            throw new NullPointerException();
        } else if (!file.canRead()) {
            throw new IllegalArgumentException("Cannot access file " + file.getPath());
        }
        RequestBody requestBody;
        if(showProgress) {
            requestBody = new ProgressRequestBody(file);
        } else {
            requestBody = RequestBody.create(MediaType.parse("text/plain"), file);
        }
        MultipartBody.Part body = MultipartBody.Part.createFormData("data", file.getName(), requestBody);

        FileUpload fileUpload = executeWithRetries(getService(PostFile.class).postFile(body));
        return fileUpload.destinationFrame;
    }


    public ParseV3 parseSetup(String[] frameIds) throws H2OException {
        if(frameIds == null) {
            throw new NullPointerException();
        }
        ParseSetupV3 guessedParams = executeWithRetries(getService(ParseSetup.class).guessSetup(frameIds));
        ParseV3 parseParams = new ParseV3();
        Util.copyFields(parseParams,guessedParams);
        parseParams.destinationFrame = Util.stringToFrameKey(guessedParams.destinationFrame);
        return parseParams;
    }

    public H2OFrame parse(ParseV3 params) throws H2OException {
        if(params == null) {
            throw new NullPointerException();
        }
        JobV3 parseJob = parseAsync(params);
        waitForJobCompletion(Util.keyToString(parseJob.key));
        return new H2OFrame(Util.keyToString(parseJob.dest));
    }

    public JobV3 parseAsync(ParseV3 params) throws H2OException {
        if(params == null) {
            throw new NullPointerException();
        }
        Call<ParseV3> parseCall = getService(Parse.class).parse(
                Util.keyToString(params.destinationFrame),
                Util.keyArrayToStringArray(params.sourceFrames),
                params.parseType,
                params.separator,
                params.singleQuotes,
                params.checkHeader,
                params.numberColumns,
                params.columnNames,
                params.columnTypes,
                params.domains,
                params.naStrings,
                params.chunkSize,
                params.deleteOnDone,
                false, //blocking = false
                Util.keyToString(params.decryptTool),
                params._excludeFields);
        ParseV3 parseResult = executeWithRetries(parseCall);
        return parseResult.job;
    }

    public H2OFrame getFrame(String frameId) throws H2OException {
        if (frameId == null) {
            throw new NullPointerException();
        }
        FramesV3 framesBody = executeWithRetries(getService(Frames.class).fetch(frameId));
        if (framesBody.frames == null || framesBody.frames.length != 1) {
            throw new H2OException("Failed to retrieve frame: " + frameId);
        }
        String returnId = framesBody.frames[0].frameId.name;
        return new H2OFrame(returnId);
    }

    public <T extends ModelSchemaBaseV3> H2OModel<T> getModel(String modelId, Class<T> modelType) throws H2OException {
        if (modelId == null) {
            throw new NullPointerException();
        }
        ModelsV3 modelsBody = executeWithRetries(getService(Models.class).fetch(modelId));
        if (modelsBody.models == null || modelsBody.models.length != 1) {
            throw new H2OException("Failed to retrieve model: " + modelId);
        }
        ModelSchemaBaseV3 model = modelsBody.models[0];
        if (model.getClass() != modelType) {
            throw new H2OException("Cannot cast model of " + model.getClass() + " to " + modelType);
        }
        return new H2OModel<>(modelType.cast(model));
    }

    public void removeAll() throws H2OException {
        executeWithRetries(getService(DKV.class).removeAll());
    }

    public void remove(String objectId) throws H2OException {
        executeWithRetries(getService(DKV.class).remove(objectId));
    }

    @Override
    public void close() {
        if (sessionKey != null) {
            try {
                Response<InitIDV3> response = getService(Sessions.class).endSession(this.sessionKey).execute();
                if (response.isSuccessful()) {
                    System.out.println("H2O session " + this.sessionKey + " closed");
                }
            } catch (IOException e) {
                System.err.println("Could not close H2O session " + this.sessionKey);
                e.printStackTrace();
            }
        }
        H2OConnection.instance = null;
    }

    public <T> T getService(Class<T> serviceType) {
        return serviceType.cast(retrofitServices.computeIfAbsent(serviceType,retrofit::create));
    }

    protected RapidsSchemaV3 evaluateRapidsExpression(String ast) throws H2OException {
        //downcast result to one of: RapidsFrameV3,RapidsNumberV3,RapidsNumbersV3,RapidsStringV3,RapidsStringsV3
        String sessionKey = getSessionKey();
        return executeWithRetries(getService(Rapids.class).rapidsExec(ast, sessionKey,null,null));
    }

    protected String newTmp() {
        return "tmp_" + UUID.randomUUID().toString();
    }


    private String getSessionKey() throws H2OException {
        if(sessionKey == null) {
            try {
                setSessionKey();
            } catch (H2OException e) {
                throw new H2OException("Unable to retrieve new session key from server",e);
            }
        }
        return this.sessionKey;
    }

    private void setSessionKey() throws H2OException {
        this.sessionKey = executeWithRetries(getService(Sessions.class).newSession4("session_key")).sessionKey;
    }

    private static boolean validateConnection(Retrofit retrofit) throws H2OException {
        int retries = 0;
        Cloud cloudService = retrofit.create(Cloud.class);
        Response<CloudV3> response;
        CloudV3 status;
        System.out.println("Attempting to connect to H2O server at " + retrofit.baseUrl());
        while(true) {
            try {
                response = cloudService.status().execute();
                if(response.isSuccessful()) {
                    status = response.body();
                    double totalFreeMem = 0;
                    int totalCores = 0;
                    int totalAllowedCores = 0;
                    for(NodeV3 node : status.nodes) {
                        totalFreeMem += (double) Math.round(node.freeMem/1073741824.0*100.0)/100.0;
                        totalCores += node.numCpus;
                        totalAllowedCores += node.cpusAllowed;
                    }
                    System.out.println(
                            "==================================\n" +
                                    "Cloud Name: " + status.cloudName + "\n" +
                                    "Cloud Version: " + status.version + "\n" +
                                    "Cloud Size: " + status.cloudSize + "\n" +
                                    "Bad Nodes: " + status.badNodes + "\n" +
                                    "Cloud Free Memory: " + totalFreeMem + " GB\n" +
                                    "Cloud Total Cores: " + totalCores + "\n" +
                                    "Cloud Allowed Cores: " + totalAllowedCores + "\n" +
                                    String.format("Cloud Uptime: %d days %d hours %d mins",
                                            (int) ((status.cloudUptimeMillis) / (1e3 * 60 * 60 * 24)),
                                            (int) (((status.cloudUptimeMillis) % (1e3 * 60 * 60 * 24)) / (1e3 * 60 * 60)),
                                            (int)  (((status.cloudUptimeMillis) % (1e3 * 60 * 60)) / (1e3 * 60)))+ "\n" +
                                    "Cloud Timezone: " + status.cloudInternalTimezone + "\n" +
                                    "Cloud Healthy: " + status.cloudHealthy + "\n" +
                                    "Consensus: " + status.consensus + "\n" +
                                    "Locked: " + status.locked + "\n" +
                                    "Client Mode: " + status.isClient + "\n" +
                                     "==================================");
                    if (!status.cloudHealthy) {
                        System.err.println("Connection established but H2O cloud not healthy");
                    }
                    return true;
                } else {
                    H2OErrorV3 error = (new Gson()).fromJson(response.errorBody().string(), H2OErrorV3.class);
                    throw new H2OException(error);
                }
            } catch(IOException e) {
                System.err.println(e.toString());
                if (++retries <= maxRetries) {
                    System.err.println("Retrying to establish H2O connection");
                } else {
                    break;
                }
            }
            try {
                //wait before retrying connection
                Thread.sleep(retryIntervalMillis);
            } catch (InterruptedException e) {
                //no interruptions expected
            }
        }
        return false;
    }

    <T> T  executeWithRetries(Call<T> call) throws H2OException {
        Response<T> response;
        int retries = 0;
        while(true) {
            try {
                response = call.execute();
                if (response.isSuccessful()) {
                    return response.body();
                } else {
                    Converter<ResponseBody, H2OErrorV3> errorV3Converter = retrofit.responseBodyConverter(H2OErrorV3.class, new Annotation[0]);
                    H2OErrorV3 error = errorV3Converter.convert(response.errorBody());
                    if(error != null) {
                        if(error instanceof H2OModelBuilderErrorV3) {
                            throw new H2OModelBuilderException((H2OModelBuilderErrorV3) error);
                        }
                        throw new H2OException(error);
                    } else {
                        throw new H2OException("Unknown error occurred");
                    }
                }
            } catch (IOException e) {
                if (++retries > maxRetries) {
                    throw new H2OException("Network error", e);
                }
                System.err.println(e.toString() + "\n Retrying API call (" + call.request().url() + ")");
                call = call.clone();
            }
            try {
                //wait before retrying connection
                Thread.sleep(retryIntervalMillis);
            } catch (InterruptedException e) {
                //no interruptions expected
            }
        }
    }


}
