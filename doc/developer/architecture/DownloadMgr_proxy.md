# Architecture: DownloadManager - ProxySelectionPolicy and HealthChecker

Introducing `ProxySelectionPolicy` and `HealthChecker` to `DownloadManager` to replace the current proxy handling.

`ProxySelectionPolicy` allows to handle the selection of a proxy for any CURL download and handles
failures of the download due to the proxy.

`HealthChecker` is its own thread that performs independent health checks if the proxies of the
current proxy group are still healthy. If not switches the proxy group.


## DownloadManager

The `Fetch` function is the function that interacts with CURL to make a new download request. 
In `SetUrlOptions(info)` the majority of CURL options for this particular download demand is set. This includes to set the proxy used to which the download request is sent.

Then, in a `do-while`-loop the download is retried until it succeeds, or runs out of options and finally fails.


Below are the positions marked in the code section where the `DownloadManager` will interact with the `ProxySelectionPolicy` interface


```cpp
Failures DownloadManager::Fetch(JobInfo *info) {
  //...
  result = PrepareDownloadDestination(info);

  //...
    CURL *handle = AcquireCurlHandle();
  //...
    SetUrlOptions(info); // --> ProxySelectionPolicy->SetProxy(info)
    int retval;
    do {
      retval = curl_easy_perform(handle);
      double elapsed;
      if (curl_easy_getinfo(handle, CURLINFO_TOTAL_TIME, &elapsed) == CURLE_OK)
      {
        perf::Xadd(counters_->sz_transfer_time,
                   static_cast<int64_t>(elapsed * 1000));
      }
    } while (VerifyAndFinalize(retval, info)); // -->ProxySelectionPolicy->HandleFailure(info)
    result = info->error_code();
    ReleaseCurlHandle(info->curl_handle());

  //...
}

```

## Proxy Classes

Following **thread-safe** classes are needed:

### 1. Proxy
Variables
- `address` (string)
- `is_healthy` (bool)

Functions
- `Proxy(address)`
- `GetAddress()`
- `SetHealthStatus(bool)`
- `GetHealthStatus()`

Have a callback function to inform HealthChecker of health changes?

### 2. ProxyGroup
Variables
- `proxies` (vector of `Proxy`)
- `is_healthy (bool)`

Functions
- `ProxyGroup(vec_proxies)`
- `GetGroupHealthStatus()`
- `SetGroupHealthStatus(bool)`

### 3. ProxyList
Variables
- `proxy_groups` (vector of `ProxyGroup`)
- `fallback_proxies` (`ProxyGroup`)
- `active_group_id` (thread-safe `int32_t`)
- `revision` (uint32_t)

Functions
- `ProxyList()`
- `AddProxyGroup(proxy_group)`
- `ClearAllProxyGroups()` - needed for `cvmfs_talk proxy set`
- `GetActiveGroupID()`
- `SetActiveGroup(int32_t id)`
- `GetNumProxyGroups()`
- `GetFallbackProxies()`
- `SetFallbackProxies(proxy_group)`
- `GetProxyGroup(size_t idx)`


## ProxySelectionPolicy

`ProxySelectionPolicy` is an interface that allows to implement different types of policies how proxy selection is performed.

Source code plugins can be based on this to implement their own policy.

### Supported policies
The following 2 policies should be provided by CVMFS

1. Default
    - 1 active `ProxyGroup`
    - `Proxies` are listed in order given by `CVMFS_HTTP_PROXY`
    - a random `Proxy` is selected
3. Sharding:
    - 1 active `ProxyGroup`
    - `Proxies` are listed in order *closest* to the hash
    - `Proxy` is selected in order

For this a new parameter should be introduced: `CVMFS_PROXY_POLICY=["default" or "sharding" or "my custom policy whatever"]`

### The Interface

The Interface consists of 2 functions 

1. `SetPreferredProxy(*JobInfo)`
2. `HandleFailure(*JobInfo)`

#### SetPreferredProxy(\*JobInfo)
Used in `SetURLOptions()` within the `DownloadManager`. Is called once for every download request.

Should set up in JobInfo for this particular download request:
- the current proxy
- order of the proxies within the active proxy group
- check that there is at least one health proxy within the active proxy group
    - if not force a switch of the proxy group


#### HandleFailure(\*JobInfo)
Depedning on failure of the download, should update the proxy selection:

- select next proxy
- check that there is at least X health proxy within the active proxy group
    - if not force a switch of the proxy group
    - switch to new proxy group requires to re-create the order of the proxies

### Changes to JobInfo object
JobInfo object must be extended with the following variables:
- `proxy_revision` (uint64_t)
    - current revision of the `ProxyList`
    - needed in case of refresh of proxy list outside of remount
      (e.g. `cvmfs_talk proxy set`)
    - increases when the active proxy group changes?
- `proxy_group_ID`
    - id of proxy group used (active proxy group at that time)
- `proxy_idx`
    - idx of current proxy used within the proxy group
- `ordered_proxy_group` (vector size_t)
    - vector of idx how the active proxy group should be accessed
- `void* proxy_policy_custom_data`

Access to set the proper proxy:
```cpp
if (proxy_revision == proxylist.GetActiveGroupID()) {
  proxy = proxylist.GetProxyGroup(proxy_group_ID)[ordered_proxy_group[proxy_idx]]
      
  if(proxy.IsHealthy()) {
    info->SetProxy(proxy)
  } else {
    findNewProxy(info)
  }
}
```


### QUESTIONS
- to save space it would be good if `proxy_revision` is increased if: either the active proxy group changes or any other changes of the proxy group setup
- should `ordered_proxy_group` be part of `proxy_policy_custom_data`?
- currently the `Host` is excluded - it can be added later

## HealthChecker
`HealthChecker` is a class that starts a thread that continuously monitors the health of proxies

It needs access to the thread-safe `ProxyList` and all its components
It will only test the current active `ProxyGroup`

If the entire `ProxyGroup` is unhealthy, switched to the next group

Variables:
- `ProxyList` - the same used by `ProxySelectionPolicy`
- `on_proxies` - vector containing healthy proxies of the active `ProxyGroup`
- `off_proxies` - vector containing unhealthy proxies of the active `ProxyGroup`
- `interval_check_on_proxies` - int32_t, how often healthy proxies should be checked
- `interval_check_off_proxies` - int32_t, how often unhealthy proxies should be checked

Functions:
- `main()` 
- `bool checkProxy()` - checks and returns true if proxy is healthy


CVMFS_PARAMETERS:
- `CVMFS_PROXY_HEALTH_CHECK=yes|no`
- `CVMFS_PROXY_HEALTHY_PROXY_CHECK_RATE=<int in sec>`
- `CVMFS_PROXY_UNHEALTHY_PROXY_CHECK_RATE=<int in sec>`

### QUESTIONS
- have a callback function whenever the health status changes from a proxy of the active group to inform the `HealthChecker`?
