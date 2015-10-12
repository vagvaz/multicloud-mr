package eu.leads.processor.common.plugins;

import com.google.common.base.Strings;
import eu.leads.processor.common.utils.FSUtilities;
import org.apache.hadoop.io.MD5Hash;
import org.vertx.java.core.json.JsonObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by vagvaz on 6/3/14. This class packages a plugin in order to upload it to the system
 */
public class PluginPackage extends JsonObject implements Serializable {
  private byte[] jar;
  private String className;
  private String id;
  private byte[] config;
  private String jarFilename;
  private String configFileName;
  private String user;

  public void setKey(String key) {
    this.key = key;
  }

  public String getKey() {
    return key;
  }

  private String key=null;

  public boolean check_MD5(MD5Hash key){
      if(key!=null && this.key!=null)
        return this.key.equals(key.toString());
      else
          return false;
  }

  public void calculate_MD5(){
      if(jarFilename!=null){
          key = null;
          try {
              FileInputStream fileInputStream= new FileInputStream(jarFilename);

              key = MD5Hash.digest(fileInputStream).toString();
              System.out.println("initializing MD5 key : " + key);
              fileInputStream.close(); //mark/reset not supported
          } catch (IOException e) {
              System.err.println("Error with " + jarFilename + " check tha the file exists and is readable." );
              e.printStackTrace();
          }

      }
  }
  public PluginPackage(String id, String className) {
    this.id = id;
    this.className = className;
  }

  public PluginPackage(String id, String className, String jarFileName) {
    this(id, className, jarFileName, null);
  }


  public PluginPackage(String id, String className, String jarFileName, String configFileName) {
    this.id = id;
    this.className = className;
    if (!Strings.isNullOrEmpty(jarFileName)) {
      //            loadJarFromFile(jarFileName);
      this.jarFilename = jarFileName;
      calculate_MD5();
    }
    if (!Strings.isNullOrEmpty(configFileName)) {
      loadConfigFromFile(configFileName);
      this.configFileName = configFileName;
    }
  }

  /**
   * This function loads the jar of a plugin.
   *
   * @param jarFileName The jar filename that we will load the plugin jar
   */
  public void loadJarFromFile(String jarFileName) {

    jar = FSUtilities.loadBytesFromFile(jarFileName);
  }

  /**
   * This function loads the configuration file of the plugin from a file
   *
   * @param configFileName the configuration filename
   */
  private void loadConfigFromFile(String configFileName) {
    config = FSUtilities.loadBytesFromFile(configFileName);
  }

  /**
   * Getter for property 'className'.
   *
   * @return Value for property 'className'.
   */
  public String getClassName() {
    return className;
  }

  /**
   * Setter for property 'className'.
   *
   * @param className Value to set for property 'className'.
   */
  public void setClassName(String className) {
    this.className = className;
  }

  /**
   * Getter for property 'id'.
   *
   * @return Value for property 'id'.
   */
  public String getId() {
    return id;
  }

  /**
   * Setter for property 'id'.
   *
   * @param id Value to set for property 'id'.
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Getter for property 'jar'.
   *
   * @return Value for property 'jar'.
   */
  public byte[] getJar() {
    return jar;
  }

  /**
   * Setter for property 'jar'.
   *
   * @param jar Value to set for property 'jar'.
   */
  public void setJar(byte[] jar) {
    this.jar = jar;
  }

  /**
   * Getter for property 'config'.
   *
   * @return Value for property 'config'.
   */
  public byte[] getConfig() {
    return config;
  }

  /**
   * Setter for property 'config'.
   *
   * @param config Value to set for property 'config'.
   */
  public void setConfig(byte[] config) {
    this.config = config;
  }

  public String getJarFilename() {
    return jarFilename;
  }

  public void setJarFilename(String jarFilename) {
    this.jarFilename = jarFilename;
  }

  public String getConfigFileName() {
    return configFileName;
  }

  public void setConfigFileName(String configFileName) {
    this.configFileName = configFileName;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

}
