package com.tableausoftware.documentation.api.rest.util.enums;

import javax.ws.rs.core.UriBuilder;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public enum Operation {
    ADD_WORKBOOK_PERMISSIONS(getApiUriBuilder().path("sites/{siteId}/workbooks/{workbookId}/permissions")),
    APPEND_FILE_UPLOAD(getApiUriBuilder().path("sites/{siteId}/fileUploads/{uploadSessionId}")),
    CREATE_GROUP(getApiUriBuilder().path("sites/{siteId}/groups")),
    INITIATE_FILE_UPLOAD(getApiUriBuilder().path("sites/{siteId}/fileUploads")),
    PUBLISH_WORKBOOK(getApiUriBuilder().path("sites/{siteId}/workbooks")),
    PUBLISH_DATA_SOURCE(getApiUriBuilder().path("/sites/{siteId}/datasources")),
    QUERY_PROJECTS(getApiUriBuilder().path("sites/{siteId}/projects")),
    QUERY_SITES(getApiUriBuilder().path("sites")),
    QUERY_WORKBOOKS(getApiUriBuilder().path("sites/{siteId}/users/{userId}/workbooks")),
    SIGN_IN(getApiUriBuilder().path("auth/signin")),
    SIGN_OUT(getApiUriBuilder().path("auth/signout"));

    private final UriBuilder m_builder;

    Operation(UriBuilder builder) {
        m_builder = builder;
    }

    public UriBuilder getUriBuilder() {
        return m_builder;
    }

    public String getUrl(Object... values) {
        return m_builder.build(values).toString();
    }
    /**
     * Creates an instance of UriBuilder, using the URL of the server specified
     * in the configuration file.
     *
     * @return the URI builder
     */
    private static UriBuilder getApiUriBuilder() {
       Properties m_properties =new Properties();
        try {
            m_properties.load(new FileInputStream("res/config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return UriBuilder.fromPath(m_properties.getProperty("server.host") + "/api/3.2");
    }
}
