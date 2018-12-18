package com.tableausoftware.documentation.api.rest;

import com.google.common.io.Files;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.file.FileDataBodyPart;
import com.tableausoftware.documentation.api.rest.bindings.*;
import com.tableausoftware.documentation.api.rest.util.RestApiUtils;
import com.tableausoftware.documentation.api.rest.util.enums.Operation;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.Test;
import sun.net.www.protocol.http.HttpURLConnection;

import javax.sql.DataSource;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.io.*;
import java.net.Proxy;
import java.net.URL;
import java.util.Properties;

/**
 * '上传数据源
 */
public class DemoPushDataSource {
    private static Logger s_logger = Logger.getLogger(Demo.class);

    private static Properties s_properties = new Properties();

    private static final RestApiUtils s_restApiUtils = RestApiUtils.getInstance();

    static {
        // Configures the logger to log to stdout
        BasicConfigurator.configure();

        // Loads the values from configuration file into the Properties instance
        try {
            s_properties.load(new FileInputStream("res/config.properties"));
        } catch (IOException e) {
            s_logger.error("Failed to load configuration files.");
        }
    }

    /**
     * 登录拆分：
     */
    public TableauCredentialsType login() {
        // Sets the username, password, and content URL, which are all required
        // in the payload of a Sign In request
        String username = s_properties.getProperty("user.admin.name");
        String password = s_properties.getProperty("user.admin.password");
        String contentUrl = s_properties.getProperty("site.default.contentUrl");

        // Signs in to server and saves the authentication token, site ID, and current user ID
        return s_restApiUtils.invokeSignIn(username, password, contentUrl);
    }

    /**
     * 登录后获取 项目内容
     */
    public void publishDataSource(TableauCredentialsType credential) {
        String currentSiteId = credential.getSite().getId();
        String currentUserId = credential.getUser().getId();

        s_logger.info(String.format("Authentication token: %s", credential.getToken()));
        s_logger.info(String.format("Site ID: %s", currentSiteId));

        ProjectListType projectListType = projectList(credential);
        ProjectType defaultProject = null;
        for (ProjectType projectType:projectListType.getProject()){
            if(projectType.getName().equals("sdas")){
                defaultProject=projectType;
            }
        }
        if (defaultProject == null)
        {
            s_logger.error("Failed to find default project");
            s_logger.info("Exiting without publishing due to previous failure");
            return;
        }

        String dataSourceName = "testvvvvv";
//        String dataSourceAddress = "F:\\work\\tableau\\test.hyper";
        String dataSourceAddress = "F:\\work\\tableau\\TableAuFile2018-12-13-test.tde";
        File dataSouce = new File(dataSourceAddress);
        // Gets whether or not to publish the workbook using file uploads
        boolean chunkedPublish = Boolean.valueOf(s_properties.getProperty("workbook.publish.chunked"));


        if(chunkedPublish){

            String url = Operation.INITIATE_FILE_UPLOAD.getUrl(currentSiteId);

            // Make a POST request with the authenticity token
            TsResponse response =s_restApiUtils.unmarshalResponse(httpConnection(url,"post" ,credential.getToken()));

            // Verifies that the response has a file upload element
            if (response.getFileUpload() != null) {
                System.out.println("Initiate file upload is successful!");

            }

            FileUploadType fileUpload = response.getFileUpload();

            // Builds the URL with the upload session id and workbook type
            UriBuilder builder = Operation.PUBLISH_DATA_SOURCE.getUriBuilder()
                    .queryParam("uploadSessionId", fileUpload.getUploadSessionId())
                    .queryParam("datasourceType", Files.getFileExtension(dataSouce.getName()))
                    .queryParam("overwrite", true)
                    .queryParam("append ", false);
            String publishUrl = builder.build(currentSiteId, fileUpload.getUploadSessionId()).toString();

            // Creates a buffer to read 100KB at a time
            byte[] buffer = new byte[100000];
            int numReadBytes = 0;

            // Reads the specified workbook and appends each chunk to the file upload
            try (FileInputStream inputStream = new FileInputStream(dataSouce.getAbsolutePath())) {
                while ((numReadBytes = inputStream.read(buffer)) != -1) {
                   s_restApiUtils.invokeAppendFileUpload(credential, currentSiteId, fileUpload.getUploadSessionId(), buffer, numReadBytes);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read the workbook file.");
            }

            // Creates the payload to publish the workbook
            TsRequest payload = createPayloadToPublishDataSource(dataSourceName, defaultProject.getId());

            // Makes a multipart POST request with specified credential's
            // authenticity token and payload
            TsResponse responseDataSource =s_restApiUtils.postMultipart(publishUrl, credential.getToken(), payload, null);

      /*      error = {ErrorType@2641}
            summary = "Bad Request"
            detail = "There was a problem publishing the file '12749:012386e71dd54a34bbe18d0f460fa358-1:0'."
            value = {char[85]@2647}
            hash = 0
            code = {BigInteger@2644} "400011"*/
            /**
             * datasource = {DatasourceType@2627}
             *  site = null
             *  project = {ProjectType@2628}
             *  owner = {UserType@2629}
             *  tags = {TagListType@2630}
             *  views = null
             *  id = "47967353-8673-4414-9234-025d3d3c9eb9"
             *  name = "testvvvvv"
             *  description = null
             *  contentUrl = "testvvvvv"
             */
            // Verifies that the response has a workbook element
            if (responseDataSource.getDatasource() != null) {
                System.out.println("successful");
            }

            // No workbook was published

        }else{
            //直接未推送成功
            String url = Operation.PUBLISH_DATA_SOURCE.getUrl(currentSiteId);

            TsRequest payload = createPayloadToPublishDataSource(dataSourceName, defaultProject.getId());

            // Makes a multipart POST request with specified credential's
            // authenticity token and payload
            BodyPart filePart = new FileDataBodyPart("tableau_datasource", dataSouce,
                    MediaType.APPLICATION_OCTET_STREAM_TYPE);
            TsResponse response = s_restApiUtils.postMultipart(url, credential.getToken(), payload, filePart);

            System.out.println("response = " + response);
            // Verifies that the response has a workbook element
            if (response.getDatasource() != null) {
                System.out.println("Publish workbook is successful!");
            }

        }


    }
    private ObjectFactory m_objectFactory = new ObjectFactory();
    /**
     * Creates the request payload used to publish a workbook.
     *
     * @param workbookName
     *            the name for the new workbook
     * @param projectId
     *            the ID of the project to publish to
     * @return the request payload
     */
    private TsRequest createPayloadToPublishDataSource(String workbookName, String projectId) {
        // Creates the parent tsRequest element
        TsRequest requestPayload = m_objectFactory.createTsRequest();

        // Creates the workbook element
        DatasourceType datasourceType = new DatasourceType();

        // Creates the project element
        ProjectType project = m_objectFactory.createProjectType();

        // Sets the target project ID
        project.setId(projectId);

        // Sets the workbook name
        datasourceType.setName(workbookName);

        // Sets the project
        datasourceType.setProject(project);

        // Adds the workbook element to the request payload
        requestPayload.setDatasource(datasourceType);

        return requestPayload;
    }

    /**
     * 获取所有项目信息
     */
    public ProjectListType projectList(TableauCredentialsType credential) {
            String currentSiteId = credential.getSite().getId();
            String currentUserId = credential.getUser().getId();
            s_logger.info(String.format("Authentication token: %s", credential.getToken()));
            s_logger.info(String.format("Site ID: %s", currentSiteId));
            String content = httpConnection(Operation.QUERY_PROJECTS.getUrl(currentSiteId),
                    "GET", credential.getToken());
            TsResponse tsResponse = s_restApiUtils.unmarshalResponse(content);

            return tsResponse.getProjects();
    }







    //http
    /**
     *
     * @param urlAddress 待连接的地址
     * @param method  连接地址的方法
     * @param token   所需的token
     * @return content 内容
     */
    public String httpConnection(String urlAddress, String method, String token) {
        if (!urlAddress.contains("http")) {
            urlAddress = "http://" + urlAddress;
        }
        ByteArrayOutputStream bso = null;
        try {
            URL url = new URL(urlAddress);
            HttpURLConnection httpsURLConnection = new HttpURLConnection(url, Proxy.NO_PROXY);
            httpsURLConnection.setRequestMethod(method.toUpperCase());
            if (token != null) {
                httpsURLConnection.setRequestProperty("X-tableau-auth", token);
            }
            httpsURLConnection.connect();
            //获取是否成功
            String responseMessage = httpsURLConnection.getResponseMessage();
            byte[] bytes = responseMessage.getBytes("UTF-8");
            //获取文件输入流，进行输出
            InputStream inputStream = httpsURLConnection.getInputStream();

            bso = new ByteArrayOutputStream();

            int len = 0;

            while ((len = inputStream.read()) != -1) {
                bso.write(len);
            }
            String content = bso.toString("UTF-8");
            System.out.println("responseMessage = " + content);
            return content;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bso != null) {
                try {
                    bso.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }



    @Test
    public void test() throws Exception {
        TableauCredentialsType login = login();
        try{
            publishDataSource(login);
        }catch (Exception e){
            s_restApiUtils.invokeSignOut(login);
        }
    }

}
