/**
 * Created by Administrator on 2019/4/23.
 */
(function () {
    var cookieUtils = {
        //向浏览器写入cookie数据
        set: function (cookiename, cookievalue) {
            var cookieText = encodeURIComponent(cookiename) + "=" + encodeURIComponent(cookievalue);
            //计算十年后的时间
            var date = new Date();
            //获取当前的时间戳
            var currentime = date.getTime();
            //计算十年时间的长度
            var tenYearLongTime = 10 * 365 * 24 * 60 * 1000;
            //累加在一起算出 十年后的时间
            var tenYearAfter = tenYearLongTime + currentime;
            //把date对象设置成十年后的时间
            date.setTime(tenYearAfter);
            cookieText += ";expires" + date.toUTCString();
            document.cookie = cookieText
        },
        /**
         *向浏览器获取cookie数据
         * document.cookie
         * uuid=123;sid=456;mid=789
         */
        get: function (cookiename) {
            var cookievalue = null;
            var cookieText = document.cookie;
            if (cookieText.indexOf(encodeURIComponent(cookiename)) > -1) {
                var items = cookieText.split(";")
                for (index in items) {
                    var kv = items[index].split("=");
                    var key = kv[0].trim();
                    var value = kv[1].trim();
                    if (key == encodeURIComponent(cookiename)) {
                        cookievalue = decodeURIComponent(value)
                    }
                }
            }
            return cookievalue
        }
    };
    /*
     *负责采集每个用户行为事件数据
     */
    var tracker = {
        clientConfig: {
            logServerUrl: "http://hadoop1/log.gif",//日志服务器地址
            sessionTimeOut: 2 * 60 * 1000, //回话 过期时间
            logVersion: "1.0"
        },
        //需要存放在cookie中的字段
        cookiekeys: {
            uuid: "uid",//用户唯一标识
            sid: "sid", //会话标识
            preVisitTime: "pre_visit_time" // 用户最近一次访问时间
        },
        /**
         * 定义需要采集的事件名称（这个一定是根据具体业务来）
         */
        events: {
            launchEvent: "e_l",//用户首次访问事件     自动触发
            pageViewEvent: "e_pv",//用户浏览页面事件  自动触发
            addCartEvent: "e_ad",//商品加入购物车事件 手动触发
            searchEvent: "e_s"//搜索事件              手动触发
        },
        /*
         *定义需要采集事件名称（这个一定是根据具体业务来）
         */
        columns: {
            eventName: "en",//事件名称
            version: "ver",//日志版本
            platform: "pl",//平台 ios Android
            sdk: "sdk",//sdk js java
            uuid: "uid",//用户唯一标识
            sessionId: "sid",//会话id
            resolution: "b_rst",//浏览器分辨率
            userAgent: "b_usa",//浏览器代理信息
            language: "l",//语言
            clientTime: "ct",//客户端时间
            currentUrl: "url",//当前页面的url
            referrerUrl: "ref",//来源url，上一个页面的url
            title: "tt",//网页标题
            keyword: "kw",//搜索关键字
            goodsId: "gid"//商品id
        },
        /**
         * 设置uuid到cookie中
         */
        setUUid: function (uuid) {
            cookieUtils.set(this.cookiekeys.uuid, uuid)
        },
        //获取uuid
        getUUid: function () {
            return cookieUtils.get(this.cookiekeys.uuid)
        },
        //设置会话sid
        setSid: function (sid) {
            cookieUtils.set(this.cookiekeys.sid,sid)
        },
        //获取sid
        getSid: function () {
            return cookieUtils.get(this.cookiekeys.sid)
        },
        /**
         * todo 会话开始了
         */
        sessionStart: function () {
            //判断会话是否存在，就是从浏览器中的cookie获取会话id，如果获取到了会话表明存在，获取不到表明不存在
            // "" null==>false
            if (!this.getSid()) {//会话不存在
                this.createNewSession()
            } else {
                //判断会话是否过期
                if (this.isSessionTimeOut()) { //会话过期了
                    this.createNewSession() //创建新的会话
                } else {
                    //更新最后一次访问时间
                    this.updatePreVisitTime()
                }
            }
            //触发pageView事件（用户浏览页面事件）
            this.pageViewEvent()
        },
        /**
         * 创建新的会话
         */
        createNewSession: function () {
            //生成会话id
            var sid = this.guid();
            //将回话保存在cookie中
            this.setSid(sid)
            //判断用户是否是首次访问（查看浏览器的cookie中是否用用户的唯一标识，如果有说明是非首次访问，否则是首次访问）
            if (!this.getUUid()) {//获取不到用户的唯一标识
                //生成用户唯一标识
                var uuid = this.guid();
                //将用户唯一标识保持到cookie中
                this.setUUid(uuid)
                //触发首次访问事件
                this.launchEvent()
            }
        }
        ,
        /*
         *定义首次访问事件
         */
        launchEvent: function () {
            //定义一个对象data，这个对象用来封装收集好的数据
            var data = {};
            //事件名称 data["en"]="e_l"
            data[this.columns.eventName] = this.events.launchEvent;
            //设置公共字段
            this.setCommonColumns(data)
            //把收集好的数据发送到服务器
            this.sendDataToLogServer(data)
        }
        ,
        /**
         * 用户浏览页面事件
         */
        pageViewEvent: function () {
            var data = {}
            //时间名称
            data[this.columns.eventName] = this.events.pageViewEvent
            //设置公开字段
            this.setCommonColumns(data)
            //当前页面的url
            data[this.columns.currentUrl] = window.location.href
            //当前页面的标题
            data[this.columns.title] = document.title
            //来源页面的url
            data[this.columns.referrerUrl] = document.referrer
            //将收集好的数据发送到服务器
            this.sendDataToLogServer(data)
        }
        ,
        /**
         * 搜索事件
         */
        searchEvent: function (keyword) {
            var data = {}
            //设置事件名称
            data[this.columns.eventName] = this.events.searchEvent;
            //设置公共字段
            this.setCommonColumns(data);
            //搜索关键词
            data[this.columns.keyword] = keyword;
            //把收集好的数据发送到服务器
            this.sendDataToLogServer(data);
        }
        ,
        /**
         * 加入购物车事件
         */
        addCartEvent: function (goodsId) {
            var data = {};
            //设置事件名称
            data[this.columns.eventName] = this.events.addCartEvent;
            //设置公共字段
            this.setCommonColumns(data);
            //商品加入购物车
            data[this.columns.goodsId] = goodsId;
            //把收集好的数据发送到服务器
            this.sendDataToLogServer(data);
        }
        ,
        /**
         * 判断会话是否过期
         */
        isSessionTimeOut: function () {
            //获取当前时间
            var currenTime = new Date().getTime()
            //获取用户最后一次访问时间
            var preVisitTime = cookieUtils.get(this.cookiekeys.preVisitTime)
            return currenTime - preVisitTime > this.clientConfig.sessionTimeOut

        }
        ,
        /**
         * 将数据发送到服务器上
         */
        sendDataToLogServer: function (data) {
            //data==> key value==> key=value&key=value&.... 数据格式
            var paramsText = ""
            for (key in data) {
                if (key && data[key]) {
                    paramsText += encodeURIComponent(key) + "=" + encodeURIComponent(data[key]) + "&"
                }
                if (paramsText) {
                    paramsText = paramsText.substring(0, paramsText.length - 1)
                }
                var url = this.clientConfig.logServerUrl + "?" + paramsText;
                var i = new Image(1,1)
                i.src = url
                //更新用户最后一次访问时间
                this.updatePreVisitTime()
            }
        },
        //更新用户最后一次访问时间
        updatePreVisitTime: function () {
            cookieUtils.set(this.cookiekeys.preVisitTime, new Date().getTime())
        },
        /**
         * 设置公共字段
         */
        setCommonColumns: function (data) {
            //sdk 版本号
            data[this.columns.version] = this.clientConfig.logVersion;
            //用户代理信息 toLowerCase转为小写
            var userAgent = window.navigator.userAgent.toLowerCase();
            data[this.columns.userAgent] = userAgent
            /**
             * indexof 用来查找子字符串是否在字符串之内，如果存在，返回对应字符串起始角标，否则返回-1
             */
            if (userAgent.indexOf("android") > -1) {
                data[this.columns.userAgent] = "android"
            } else if (userAgent.indexOf("iphone") > -1) {
                data[this.columns.userAgent] = "iphone"
            } else {
                data[this.columns.userAgent] = "pc"
            }
            data[this.columns.sdk] = "js"
            //用户唯一标识
            data[this.columns.uuid] = this.getUUid()
            //会话id
            data[this.columns.sessionId] = this.getSid()
            //浏览器分辨率
            data[this.columns.resolution] = window.screen.width + "*" + window.screen.height
            //客户端语言
            data[this.columns.language] = window.navigator.language
            //客户端当前时间
            data[this.columns.clientTime] = new Date().getTime()
        },
        /**
         * 生成唯一标识guid
         */
        guid: function () {
            return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
                var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
        }
    };
 //给window对象注入属性_AE_
    window._AE_={
        sessionStart :function () {
          tracker.sessionStart()
        },
        searchEvent : function (keyword) {
          tracker.searchEvent(keyword)
        },
        addCartEvent : function (pid) {
          tracker.addCartEvent(pid)
        }
    };
    window._AE_.sessionStart()
})();