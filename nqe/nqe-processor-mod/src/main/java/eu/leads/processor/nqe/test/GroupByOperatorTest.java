package eu.leads.processor.nqe.test;

import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.ActionStatus;
import eu.leads.processor.infinispan.operators.GroupByOperator;
import org.infinispan.Cache;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.Map;
import java.util.UUID;

/**
 * Created by vagvaz on 9/26/14.
 */
public class GroupByOperatorTest {

   public static void main(String[] args) {
      LQPConfiguration.initialize();
      String configString = "{\n" +
                                    "      \"configuration\" : {\n" +
                                    "        \"type\" : \"GROUP_BY\",\n" +
                                    "        \"body\" : {\n" +
                                    "\t  \"groupingColumns\" : [ {\n" +
                                    "            \"name\" : \"domainName\",\n" +
                                    "            \"dataType\" : {\n" +
                                    "              \"type\" : \"TEXT\"\n" +
                                    "            }\n" +
                                    "          } ],\n" +
                                    "                    \"aggrFunctions\" : [ {\n" +
                                    "            \"instance\" : {\n" +
                                    "              \"class\" : \"org.apache.tajo.engine.function.builtin.AvgInt\",\n" +
                                    "              \"body\" : {\n" +
                                    "                \"definedParams\" : [ {\n" +
                                    "                  \"name\" : \"expr\",\n" +
                                    "                  \"dataType\" : {\n" +
                                    "                    \"type\" : \"INT8\"\n" +
                                    "                  }\n" +
                                    "                } ]\n" +
                                    "              }\n" +
                                    "            },\n" +
                                    "            \"firstPhase\" : false,\n" +
                                    "            \"funcDesc\" : {\n" +
                                    "              \"signature\" : \"min\",\n" +
                                    "              \"funcClass\" : \"org.apache.tajo.engine.function.builtin.AvgInt\",\n" +
                                    "              \"funcType\" : \"AGGREGATION\",\n" +
                                    "              \"returnType\" : {\n" +
                                    "                \"type\" : \"FLOAT8\"\n" +
                                    "              },\n" +
                                    "              \"params\" : [ {\n" +
                                    "                \"type\" : \"INT4\"\n" +
                                    "              } ],\n" +
                                    "              \"description\" : \"the mean of a set of numbers.\",\n" +
                                    "              \"detail\" : \"\",\n" +
                                    "              \"example\" : \"> SELECT avg(expr);\"\n" +
                                    "            },\n" +
                                    "            \"argEvals\" : [ {\n" +
                                    "              \"type\" : \"FIELD\",\n" +
                                    "              \"body\" : {\n" +
                                    "                \"column\" : {\n" +
                                    "                  \"name\" : \"responseTime\",\n" +
                                    "                  \"dataType\" : {\n" +
                                    "                    \"type\" : \"INT4\"\n" +
                                    "                  }\n" +
                                    "                },\n" +
                                    "                \"fieldId\" : -1,\n" +
                                    "                \"type\" : \"FIELD\"\n" +
                                    "              }\n" +
                                    "            } ],\n" +
                                    "            \"type\" : \"AGG_FUNCTION\"\n" +
                                    "          }],\n" +
                                    "          \"targets\" : [ {\n" +
                                    "            \"expr\" : {\n" +
                                    "              \"type\" : \"FIELD\",\n" +
                                    "              \"body\" : {\n" +
                                    "                \"column\" : {\n" +
                                    "                  \"name\" : \"domainName\",\n" +
                                    "                  \"dataType\" : {\n" +
                                    "                    \"type\" : \"TEXT\"\n" +
                                    "                  }\n" +
                                    "                },\n" +
                                    "                \"fieldId\" : -1,\n" +
                                    "                \"type\" : \"FIELD\"\n" +
                                    "              }\n" +
                                    "            },\n" +
                                    "            \"column\" : {\n" +
                                    "              \"name\" : \"domainName\",\n" +
                                    "              \"dataType\" : {\n" +
                                    "                \"type\" : \"TEXT\"\n" +
                                    "              }\n" +
                                    "            }\n" +
                                    "          },\n" +
                                    "          {\n" +
                                    "            \"expr\" : {\n" +
                                    "              \"type\" : \"FIELD\",\n" +
                                    "              \"body\" : {\n" +
                                    "                \"column\" : {\n" +
                                    "                  \"name\" : \"url\",\n" +
                                    "                  \"dataType\" : {\n" +
                                    "                    \"type\" : \"TEXT\"\n" +
                                    "                  }\n" +
                                    "                },\n" +
                                    "                \"fieldId\" : -1,\n" +
                                    "                \"type\" : \"FIELD\"\n" +
                                    "              }\n" +
                                    "            },\n" +
                                    "            \"column\" : {\n" +
                                    "              \"name\" : \"url\",\n" +
                                    "              \"dataType\" : {\n" +
                                    "                \"type\" : \"TEXT\"\n" +
                                    "              }\n" +
                                    "            }\n" +
                                    "          },\n" +
                                    "          {\n" +
                                    "            \"expr\" : {\n" +
                                    "              \"type\" : \"FIELD\",\n" +
                                    "              \"body\" : {\n" +
                                    "                \"column\" : {\n" +
                                    "                  \"name\" : \"?min\",\n" +
                                    "                  \"dataType\" : {\n" +
                                    "                    \"type\" : \"FLOAT8\"\n" +
                                    "                  }\n" +
                                    "                },\n" +
                                    "                \"fieldId\" : -1,\n" +
                                    "                \"type\" : \"FIELD\"\n" +
                                    "              }\n" +
                                    "            },\n" +
                                    "            \"column\" : {\n" +
                                    "              \"name\" : \"?min\",\n" +
                                    "              \"dataType\" : {\n" +
                                    "                \"type\" : \"FLOAT8\"\n" +
                                    "              }\n" +
                                    "            }\n" +
                                    "          }],\n" +
                                    "          \"broadcastTable\" : false,\n" +
                                    "          \"pid\" : 0,\n" +
                                    "          \"type\" : \"GROUP_BY\",\n" +
                                    "          \"inputSchema\" : {\n" +
                                    "            \"fields\" : [  {\n" +
                                    "                \"name\" : \"url\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"TEXT\"\n" +
                                    "                }\n" +
                                    "              },  {\n" +
                                    "                \"name\" : \"domainName\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"TEXT\"\n" +
                                    "                }\n" +
                                    "              }, { \n" +
                                    "                \"name\" : \"content\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"TEXT\"\n" +
                                    "                }\n" +
                                    "              }, {\n" +
                                    "                \"name\" : \"responseCode\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"INT32\"\n" +
                                    "                }\n" +
                                    "              }, {\n" +
                                    "                \"name\" : \"charset\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"TEXT\"\n" +
                                    "                }\n" +
                                    "              },\n" +
                                    "               {\n" +
                                    "                \"name\" : \"responseTime\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"INT32\"\n" +
                                    "                }\n" +
                                    "              },\n" +
                                    "               {\n" +
                                    "                \"name\" : \"links\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"BLOB\"\n" +
                                    "                }\n" +
                                    "              },\n" +
                                    "               {\n" +
                                    "                \"name\" : \"title\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"TEXT\"\n" +
                                    "                }\n" +
                                    "              }\n" +
                                    "              ],\n" +
                                    "            \"fieldsByQualifiedName\" : {\n" +
                                    "              \"default.dept.deptname\" : 0,\n" +
                                    "              \"default.dept.tmsp\" : 2,\n" +
                                    "              \"default.dept.manager\" : 1\n" +
                                    "            },\n" +
                                    "            \"fieldsByName\" : {\n" +
                                    "              \"manager\" : [ 1 ],\n" +
                                    "              \"deptname\" : [ 0 ],\n" +
                                    "              \"tmsp\" : [ 2 ]\n" +
                                    "            }\n" +
                                    "          },\n" +
                                    "          \"outputSchema\" : {\n" +
                                    "            \"fields\" : [    {\n" +
                                    "                \"name\" : \"domainName\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"TEXT\"\n" +
                                    "                }\n" +
                                    "              }, {\n" +
                                    "                \"name\" : \"responseCode\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"INT32\"\n" +
                                    "                }\n" +
                                    "              }, \n" +
                                    "               {\n" +
                                    "                \"name\" : \"responseTime\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"INT32\"\n" +
                                    "                }\n" +
                                    "              },\n" +
                                    "               \n" +
                                    "               {\n" +
                                    "                \"name\" : \"title\",\n" +
                                    "                \"dataType\" : {\n" +
                                    "                  \"type\" : \"TEXT\"\n" +
                                    "                }\n" +
                                    "              }\n" +
                                    "              ],\n" +
                                    "            \"fieldsByQualifiedName\" : {\n" +
                                    "              \"default.dept.deptname\" : 0\n" +
                                    "            },\n" +
                                    "            \"fieldsByName\" : {\n" +
                                    "              \"deptname\" : [ 0 ]\n" +
                                    "            }\n" +
                                    "          },\n" +
                                    "          \"cost\" : 0.0\n" +
                                    "        }\n" +
                                    "        },\n" +
                                    "      \"nodetype\" : \"GROUP_BY\",\n" +
                                    "      \"id\" : \"groupby-test\",\n" +
                                    "      \"status\" : \"PENDING\",\n" +
                                    "      \"site\" : \"\",\n" +
                                    "      \"output\" : \"queryId-custom.3\",\n" +
                                    "      \"inputs\" : [\"crawler.default.cache\"]\n" +
                                    "}";

      System.out.println("Json bo\n"+ configString);
      JsonObject confg = new JsonObject(configString);
      InfinispanManager manager = InfinispanClusterSingleton.getInstance().getManager();
      addUrls(manager);
      Action action  = createNewAction();
      action.setData(confg);

      GroupByOperator operator = new GroupByOperator(null, InfinispanClusterSingleton.getInstance().getManager(),null,action);
      operator.init(action.getData());
      operator.execute();
      try {
         operator.join();
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
      operator.cleanup();
      Map<String,String> ouptut = manager.getPersisentCache(operator.getOutput());
      PrintUtilities.saveMapToFile(ouptut,"/home/vagvaz/test/groupbyOutput.json");

   }

   private static void addUrls(InfinispanManager manager) {
      String w1 = "{\n" +
                          "  \"url\" : \"http://news.yahoo.com/comics/working-daze-slideshow/\",\n" +
                          "  \"domainName\" : \"bbc.com\",\n" +
                          "  \"content\" : \"Home Mail News News \","+
                          " \"headers\" : {\n" +
                          "    \"content-type\" : \"text/html;charset=utf-8\",\n" +
                          "    \"\" : \"HTTP/1.1 200 OK\",\n" +
                          "    \"x-frame-options\" : \"SAMEORIGIN\",\n" +
                          "    \"connection\" : \"close\",\n" +
                          "    \"via\" : \"HTTP/1.1 ncache1.global.media.ir2.yahoo.com (MediaFEValidator/1.0), 1.1 ncache1.global.media.ir2.yahoo.com-2:3128 (squid/2.7.STABLE9), 1.1 ncache11.global.media.ir2.yahoo.com:4080 (squid/2.7.STABLE9), http/1.1 yts302.global.media.ir2.yahoo.com (ApacheTrafficServer [cMsSfW]), http/1.1 r07.ycpi.ams.yahoo.net (ApacheTrafficServer [cMsSf ])\",\n" +
                          "    \"transfer-encoding\" : \"chunked\",\n" +
                          "    \"content-encoding\" : \"gzip\",\n" +
                          "    \"x-cache\" : \"MISS from ncache11.global.media.ir2.yahoo.com\",\n" +
                          "    \"date\" : \"Thu, 25 Sep 2014 07:03:33 GMT\",\n" +
                          "    \"p3p\" : \"policyref=\\\"http://info.yahoo.com/w3c/p3p.xml\\\", CP=\\\"CAO DSP COR CUR ADM DEV TAI PSA PSD IVAi IVDi CONi TELo OTPi OUR DELi SAMi OTRi UNRi PUBi IND PHY ONL UNI PUR FIN COM NAV INT DEM CNT STA POL HEA PRE LOC GOV\\\"\",\n" +
                          "    \"cache-control\" : \"max-age=0, private\",\n" +
                          "    \"expires\" : \"-1\",\n" +
                          "    \"x-cache-lookup\" : \"MISS from ncache11.global.media.ir2.yahoo.com:4080\",\n" +
                          "    \"x-yahoo-request-id\" : \"81mr215a27fi5\",\n" +
                          "    \"age\" : \"524\",\n" +
                          "    \"server\" : \"ATS\"\n" +
                          "  },\n" +
                          "  \"responseCode\" : 200,\n" +
                          "  \"charset\" : \"utf-8\",\n" +
                          "  \"responseTime\" : 324,\n" +
                          "  \"links\" : [ \"http://news.yahoo.com/comics/working-daze-slideshow/\", \"http://www.yahoo.com/news/comics/working-daze-slideshow/\", \"http://l.yimg.com/os/mit/media/p/presentation/images/icons/default-apple-touch-icon-1636137.png\", \"http://l.yimg.com/os/mit/media/p/presentation/images/icons/default-apple-touch-icon-high-1636137.png\", \"http://l.yimg.com/os/mit/media/p/presentation/images/icons/default-apple-touch-icon-tablet-1636137.png\", \"http://l.yimg.com/os/mit/media/p/presentation/images/icons/default-apple-touch-icon-tablet-high-1636137.png\", \"http://l.yimg.com/zz/combo?os/stencil/2.0.11/styles.css&os/mit/media/p/content/other/ad-controller-desktop-min-2e496c7.css&os/mit/media/p/content/base/v2-master-min-8180379.css&os/mit/media/p/content/base/v2-desktop-min-8180379.css&os/mit/media/p/common/stencil-fix-min-56d3a2e.css&os/mit/media/p/content/grids/v2sg-desktop-min-a41d27c.css&os/mit/media/p/content/overrides/classic-overrides-min-c644875.css&os/mit/media/p/content/overrides/stencil-v2s-reset-overrides-min-0a29171.css&os/mit/media/m/header/header-uh3-desktop-min-0724437.css&os/mit/media/m/ads/ads-min-21b51bb.css&os/mit/media/m/navigation/content-navigation-stencil-desktop-min-a8a04a0.css\", \"http://l.yimg.com/zz/combo?os/mit/media/m/content/sfl-notifier-min-ee15495.css&os/mit/media/m/content_social/media-content-follow-property-min-0b49aeb.css&os/mit/media/m/content_social/media-content-follow-property-desktop-min-d86f135.css&os/mit/media/m/news/comiclist-desktop-min-a623402.css&yui:3.16.0/build/overlay/assets/skins/sam/overlay.css&os/mit/media/m/content_social/media-content-share-buttons-int-min-0116754.css\", \"http://l.yimg.com/zz/combo?os/mit/media/themes/v2_base/base-min-41cfe10.css&os/mit/media/themes/v2_base/base-imagery-min-bec5796.css&os/mit/media/themes/v2_navyblue/theme-min-347220d.css&os/mit/media/themes/v2_navyblue/theme-imagery-min-5149db7.css&os/mit/media/p/news/region/US-min-7e5611a.css\", \"http://l.yimg.com/zz/combo?kx/yucs/uh_common/meta/3/css/meta-min.css&kx/yucs/uh3/get-the-app/151/css/get_the_app-min.css&kx/yucs/uh3s/uh/236/css/uh-gs-grid-min.css\", \"https://www.yahoo.com/\", \"https://mail.yahoo.com/\", \"http://news.yahoo.com/\", \"http://sports.yahoo.com/\", \"http://finance.yahoo.com/\", \"https://weather.yahoo.com/\", \"https://games.yahoo.com/\", \"https://groups.yahoo.com/\", \"https://answers.yahoo.com/\", \"https://screen.yahoo.com/\", \"https://www.flickr.com/\", \"https://mobile.yahoo.com/\", \"http://everything.yahoo.com/\", \"https://celebrity.yahoo.com/\", \"https://www.yahoo.com/movies\", \"https://music.yahoo.com/\", \"https://tv.yahoo.com/\", \"https://www.yahoo.com/health\", \"https://www.yahoo.com/style\", \"https://www.yahoo.com/beauty\", \"https://www.yahoo.com/food\", \"https://www.yahoo.com/tech\", \"http://shopping.yahoo.com/\", \"https://www.yahoo.com/travel\", \"https://autos.yahoo.com/\", \"https://homes.yahoo.com/own-rent/\", \"http://news.yahoo.com\", \"https://login.yahoo.com/config/login?.src=yn&.intl=us&.lang=en-US&.done=http://news.yahoo.com/comics/working-daze-slideshow/\", \"https://mail.yahoo.com/?.intl=us&.lang=en-US&.src=ym\", \"http://news.yahoo.com/comics/working-daze-slideshow/\", \"https://edit.yahoo.com/mc2.0/eval_profile?.intl=us&.lang=en-US&.done=http://news.yahoo.com/comics/working-daze-slideshow/&.src=yn&.intl=us&.lang=en-US\", \"https://help.yahoo.com/l/us/yahoo/news_global/\", \"http://feedback.yahoo.com/forums/204992\", \"https://www.facebook.com/yahoonews\", \"https://twitter.com/yahoonews\", \"http://yahoonews.tumblr.com\", \"https://plus.google.com/+YahooNews\", \"https://www.flickr.com/photos/yahoonews\", \"http://news.yahoo.com/comics/working-daze-slideshow/\", \"http://news.yahoo.com/\", \"http://news.yahoo.com/us/\", \"http://news.yahoo.com/world/\", \"http://news.yahoo.com/politics/\", \"http://www.yahoo.com/tech/\", \"http://news.yahoo.com/science/\", \"http://news.yahoo.com/health/\", \"http://news.yahoo.com/odd-news/\", \"http://news.yahoo.com/local/\", \"http://news.yahoo.com/dear-abby/\", \"http://news.yahoo.com/comics/\", \"http://news.yahoo.com/abc-news/\", \"http://news.yahoo.com/originals/\", \"http://news.yahoo.com/yahoo_news_photos/\" ]," +
                          "  \"title\" : \" Working Daze\"\n" +
                          "}";
      String w2 = "{\n" +
                          "  \"url\" : \"http://bbc.com/comics/working-daze-slideshow/\",\n" +
                          "  \"domainName\" : \"bbc.com\",\n" +
                          "  \"content\" : \"Home Mail News News \","+
                          " \"headers\" : {\n" +
                          "    \"content-type\" : \"text/html;charset=utf-8\",\n" +
                          "    \"\" : \"HTTP/1.1 200 OK\",\n" +
                          "    \"x-frame-options\" : \"SAMEORIGIN\",\n" +
                          "    \"connection\" : \"close\",\n" +
                          "    \"via\" : \"HTTP/1.1 ncache1.global.media.ir2.yahoo.com (MediaFEValidator/1.0), 1.1 ncache1.global.media.ir2.yahoo.com-2:3128 (squid/2.7.STABLE9), 1.1 ncache11.global.media.ir2.yahoo.com:4080 (squid/2.7.STABLE9), http/1.1 yts302.global.media.ir2.yahoo.com (ApacheTrafficServer [cMsSfW]), http/1.1 r07.ycpi.ams.yahoo.net (ApacheTrafficServer [cMsSf ])\",\n" +
                          "    \"transfer-encoding\" : \"chunked\",\n" +
                          "    \"content-encoding\" : \"gzip\",\n" +
                          "    \"x-cache\" : \"MISS from ncache11.global.media.ir2.yahoo.com\",\n" +
                          "    \"date\" : \"Thu, 25 Sep 2014 07:03:33 GMT\",\n" +
                          "    \"p3p\" : \"policyref=\\\"http://info.yahoo.com/w3c/p3p.xml\\\", CP=\\\"CAO DSP COR CUR ADM DEV TAI PSA PSD IVAi IVDi CONi TELo OTPi OUR DELi SAMi OTRi UNRi PUBi IND PHY ONL UNI PUR FIN COM NAV INT DEM CNT STA POL HEA PRE LOC GOV\\\"\",\n" +
                          "    \"cache-control\" : \"max-age=0, private\",\n" +
                          "    \"expires\" : \"-1\",\n" +
                          "    \"x-cache-lookup\" : \"MISS from ncache11.global.media.ir2.yahoo.com:4080\",\n" +
                          "    \"x-yahoo-request-id\" : \"81mr215a27fi5\",\n" +
                          "    \"age\" : \"524\",\n" +
                          "    \"server\" : \"ATS\"\n" +
                          "  },\n" +
                          "  \"responseCode\" : 200,\n" +
                          "  \"charset\" : \"utf-8\",\n" +
                          "  \"responseTime\" : 200,\n" +
                          "  \"links\" : [ \"http://news.yahoo.com/comics/working-daze-slideshow/\", \"http://www.yahoo.com/news/comics/working-daze-slideshow/\", \"http://l.yimg.com/os/mit/media/p/presentation/images/icons/default-apple-touch-icon-1636137.png\", \"http://l.yimg.com/os/mit/media/p/presentation/images/icons/default-apple-touch-icon-high-1636137.png\", \"http://l.yimg.com/os/mit/media/p/presentation/images/icons/default-apple-touch-icon-tablet-1636137.png\", \"http://l.yimg.com/os/mit/media/p/presentation/images/icons/default-apple-touch-icon-tablet-high-1636137.png\", \"http://l.yimg.com/zz/combo?os/stencil/2.0.11/styles.css&os/mit/media/p/content/other/ad-controller-desktop-min-2e496c7.css&os/mit/media/p/content/base/v2-master-min-8180379.css&os/mit/media/p/content/base/v2-desktop-min-8180379.css&os/mit/media/p/common/stencil-fix-min-56d3a2e.css&os/mit/media/p/content/grids/v2sg-desktop-min-a41d27c.css&os/mit/media/p/content/overrides/classic-overrides-min-c644875.css&os/mit/media/p/content/overrides/stencil-v2s-reset-overrides-min-0a29171.css&os/mit/media/m/header/header-uh3-desktop-min-0724437.css&os/mit/media/m/ads/ads-min-21b51bb.css&os/mit/media/m/navigation/content-navigation-stencil-desktop-min-a8a04a0.css\", \"http://l.yimg.com/zz/combo?os/mit/media/m/content/sfl-notifier-min-ee15495.css&os/mit/media/m/content_social/media-content-follow-property-min-0b49aeb.css&os/mit/media/m/content_social/media-content-follow-property-desktop-min-d86f135.css&os/mit/media/m/news/comiclist-desktop-min-a623402.css&yui:3.16.0/build/overlay/assets/skins/sam/overlay.css&os/mit/media/m/content_social/media-content-share-buttons-int-min-0116754.css\", \"http://l.yimg.com/zz/combo?os/mit/media/themes/v2_base/base-min-41cfe10.css&os/mit/media/themes/v2_base/base-imagery-min-bec5796.css&os/mit/media/themes/v2_navyblue/theme-min-347220d.css&os/mit/media/themes/v2_navyblue/theme-imagery-min-5149db7.css&os/mit/media/p/news/region/US-min-7e5611a.css\", \"http://l.yimg.com/zz/combo?kx/yucs/uh_common/meta/3/css/meta-min.css&kx/yucs/uh3/get-the-app/151/css/get_the_app-min.css&kx/yucs/uh3s/uh/236/css/uh-gs-grid-min.css\", \"https://www.yahoo.com/\", \"https://mail.yahoo.com/\", \"http://news.yahoo.com/\", \"http://sports.yahoo.com/\", \"http://finance.yahoo.com/\", \"https://weather.yahoo.com/\", \"https://games.yahoo.com/\", \"https://groups.yahoo.com/\", \"https://answers.yahoo.com/\", \"https://screen.yahoo.com/\", \"https://www.flickr.com/\", \"https://mobile.yahoo.com/\", \"http://everything.yahoo.com/\", \"https://celebrity.yahoo.com/\", \"https://www.yahoo.com/movies\", \"https://music.yahoo.com/\", \"https://tv.yahoo.com/\", \"https://www.yahoo.com/health\", \"https://www.yahoo.com/style\", \"https://www.yahoo.com/beauty\", \"https://www.yahoo.com/food\", \"https://www.yahoo.com/tech\", \"http://shopping.yahoo.com/\", \"https://www.yahoo.com/travel\", \"https://autos.yahoo.com/\", \"https://homes.yahoo.com/own-rent/\", \"http://news.yahoo.com\", \"https://login.yahoo.com/config/login?.src=yn&.intl=us&.lang=en-US&.done=http://news.yahoo.com/comics/working-daze-slideshow/\", \"https://mail.yahoo.com/?.intl=us&.lang=en-US&.src=ym\", \"http://news.yahoo.com/comics/working-daze-slideshow/\", \"https://edit.yahoo.com/mc2.0/eval_profile?.intl=us&.lang=en-US&.done=http://news.yahoo.com/comics/working-daze-slideshow/&.src=yn&.intl=us&.lang=en-US\", \"https://help.yahoo.com/l/us/yahoo/news_global/\", \"http://feedback.yahoo.com/forums/204992\", \"https://www.facebook.com/yahoonews\", \"https://twitter.com/yahoonews\", \"http://yahoonews.tumblr.com\", \"https://plus.google.com/+YahooNews\", \"https://www.flickr.com/photos/yahoonews\", \"http://news.yahoo.com/comics/working-daze-slideshow/\", \"http://news.yahoo.com/\", \"http://news.yahoo.com/us/\", \"http://news.yahoo.com/world/\", \"http://news.yahoo.com/politics/\", \"http://www.yahoo.com/tech/\", \"http://news.yahoo.com/science/\", \"http://news.yahoo.com/health/\", \"http://news.yahoo.com/odd-news/\", \"http://news.yahoo.com/local/\", \"http://news.yahoo.com/dear-abby/\", \"http://news.yahoo.com/comics/\", \"http://news.yahoo.com/abc-news/\", \"http://news.yahoo.com/originals/\", \"http://news.yahoo.com/yahoo_news_photos/\" ]," +
                          "  \"title\" : \" Working Daze\"\n" +
                          "}";



      Cache c = (Cache) manager.getPersisentCache(StringConstants.CRAWLER_DEFAULT_CACHE);
      JsonObject ww1 = new JsonObject(w1);
      JsonObject ww2 = new JsonObject(w2);
      c.put("s1",ww1.toString());
      c.put("s2",ww2.toString());



   }

   private static Action createNewAction() {
      Action result = new Action();
      result.setId(UUID.randomUUID().toString());
      result.setTriggered(UUID.randomUUID().toString());
      result.setComponentType("testign");
      result.setStatus(ActionStatus.PENDING.toString());
      result.setTriggers(new JsonArray());
      result.setOwnerId("testId");
      result.setProcessedBy("");
      result.setDestination("");
      result.setData(new JsonObject());
      result.setResult(new JsonObject());
      result.setLabel("");
      result.setCategory("");
      return result;
   }
}