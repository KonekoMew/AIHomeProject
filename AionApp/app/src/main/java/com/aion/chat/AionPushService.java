package com.aion.chat;

import android.app.AlarmManager;
import android.app.KeyguardManager;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothGatt;
import android.bluetooth.BluetoothGattCallback;
import android.bluetooth.BluetoothGattCharacteristic;
import android.bluetooth.BluetoothGattDescriptor;
import android.bluetooth.BluetoothGattService;
import android.bluetooth.BluetoothManager;
import android.bluetooth.BluetoothProfile;
import android.bluetooth.le.BluetoothLeScanner;
import android.bluetooth.le.ScanCallback;
import android.bluetooth.le.ScanRecord;
import android.bluetooth.le.ScanResult;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.ServiceInfo;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.net.NetworkRequest;
import android.net.wifi.WifiManager;
import android.os.Build;
import android.os.IBinder;
import android.os.PowerManager;
import android.os.SystemClock;
import android.util.Base64;
import android.util.DisplayMetrics;
import android.util.Log;

import androidx.annotation.Nullable;
import androidx.core.app.NotificationCompat;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import android.media.AudioAttributes;
import android.media.MediaPlayer;

import android.Manifest;
import android.content.pm.PackageManager;
import android.location.Location;
import android.location.LocationListener;
import android.location.LocationManager;
import android.os.Bundle;
import androidx.core.content.ContextCompat;
import okhttp3.MediaType;
import okhttp3.RequestBody;

import android.app.usage.UsageStats;
import android.app.usage.UsageStatsManager;
import android.app.usage.UsageEvents;
import android.provider.Settings;

import android.content.BroadcastReceiver;
import android.content.IntentFilter;
import android.graphics.Bitmap;
import android.graphics.PixelFormat;
import android.hardware.display.DisplayManager;
import android.hardware.display.VirtualDisplay;
import android.media.Image;
import android.media.ImageReader;
import android.media.projection.MediaProjection;
import android.media.projection.MediaProjectionManager;

import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;

import android.os.Handler;
import android.os.Looper;

import java.text.SimpleDateFormat;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Locale;
import java.util.UUID;

/**
 * 前台服务 — OkHttp WebSocket 长连接
 * 针对 vivo/OPPO 等 ROM 做了适配：
 * 1. Thread.sleep 心跳（不依赖 Handler/Looper）
 * 2. ConnectivityManager.NetworkCallback 监听网络变化
 * 3. synchronized connectWebSocket 防并发竞争
 * 4. onFailure 不阻塞 OkHttp 回调线程
 * 5. fullScreenIntent 闹铃通知（锁屏也能亮屏弹出）
 */
public class AionPushService extends Service {

    private static final String TAG = "AionPush";

    private static final String CH_KEEPALIVE = "aion_keepalive";
    private static final String CH_MESSAGE   = "aion_message";
    private static final String CH_ALARM     = "aion_alarm";

    private static final int NOTIF_FOREGROUND = 1;
    private static final int NOTIF_MSG_BASE   = 1000;

    private static final long HEARTBEAT_MS  = 45_000;  // 45s 心跳（省电）
    private static final long HEALTH_TIMEOUT = 120_000; // 120s 无消息 → 重连

    private OkHttpClient client;
    private volatile WebSocket webSocket;
    private volatile String serverUrl;
    private int notifCounter = 0;

    private final AtomicInteger wsGeneration = new AtomicInteger(0);
    private final AtomicBoolean wsConnected = new AtomicBoolean(false);

    private volatile int reconnectDelay = 3000;
    private static final int MAX_RECONNECT_DELAY = 30000;
    private volatile boolean shouldRun = true;
    private volatile boolean isForegroundActive = false;

    private PowerManager.WakeLock wakeLock;
    private WifiManager.WifiLock wifiLock;
    private Thread heartbeatThread;
    private MediaPlayer mediaPlayer;

    private volatile int msgReceived = 0;
    private volatile long lastMessageTime = 0;

    private ConnectivityManager connectivityManager;
    private ConnectivityManager.NetworkCallback networkCallback;

    // ── ESP32-CAM 桥接 ──
    private volatile boolean esp32BridgeActive = false;
    private volatile String esp32CaptureUrl = "";
    private Thread esp32BridgeThread;

    // ── 定位上报 ──
    private static final long LOCATION_INTERVAL = 10 * 60_000;          // 统一 10 分钟（服务端做智能过滤，非每次都调 API）
    private static final long LOCATION_INTERVAL_DISABLED = 10 * 60_000; // 功能未启用/静默时段时低频轮询开关状态
    private Thread locationThread;
    private volatile long locationInterval = LOCATION_INTERVAL;
    private LocationManager locationManager;
    private volatile Location lastKnownLocation;
    private volatile boolean locationEnabled = false;  // 服务端定位开关状态

    // ── 戒指心率后台同步 ──
    private static final long RING_SYNC_INTERVAL = 10 * 60_000L;
    private static final int RING_SYNC_OFFSET_MINUTE = 2; // 戒指整 10 分钟测量后，错后 2 分钟拉取
    private Thread ringSyncThread;
    private RingBackgroundSync ringBackgroundSync;

    // ── 活动上报 ──
    private static final long ACTIVITY_INTERVAL = 60_000;  // 60秒检测一次前台应用
    private static final long ACTIVITY_RE_REPORT_MS = 5 * 60_000;  // 同一App超过5分钟重新上报
    private Thread activityThread;
    private volatile String lastReportedApp = "";
    private volatile long lastReportedTime = 0;
    private volatile boolean screenOn = true;
    private BroadcastReceiver screenReceiver;

    // ── 无障碍服务自动恢复（需 WRITE_SECURE_SETTINGS 权限，通过 ADB 授予）──
    private volatile long lastAccessibilityRecoverAt = 0;
    private static final long ACCESSIBILITY_RECOVER_COOLDOWN = 5_000; // 恢复操作冷却 5 秒

    // ── 手机屏幕截图（MediaProjection，需要用户显式授权）──
    public static final String ACTION_START_PHONE_SCREEN = "start_phone_screen_projection";
    public static final String ACTION_STOP_PHONE_SCREEN = "stop_phone_screen_projection";
    public static final String ACTION_TEST_ACCESSIBILITY_SCREEN = "test_accessibility_screen";
    public static final String EXTRA_RESULT_CODE = "result_code";
    public static final String EXTRA_RESULT_DATA = "result_data";
    private final Object phoneScreenLock = new Object();
    private MediaProjectionManager projectionManager;
    private MediaProjection mediaProjection;
    private VirtualDisplay phoneScreenDisplay;
    private ImageReader phoneScreenReader;
    private volatile boolean phoneScreenEnabled = false;
    private volatile long lastPhoneCaptureAt = 0;

    // ── 步数计数 ──
    // 使用 TYPE_STEP_COUNTER（硬件累计步数，低功耗），搭载定位线程 10 分钟上报
    // 凌晨 5:00 重置（逻辑日期以 5:00 为分界，适应晚睡作息）
    // 重启检测：currentCounter < lastKnownCounter 时把上一 boot 周期走的步数补偿到 rebootOffset
    private SensorManager sensorManager;
    private Sensor stepSensor;
    private volatile float latestStepCounter = -1;  // 传感器最新值（开机累计）
    private volatile int serverStepRestore = -1;    // 服务端恢复的步数（重装 APK 后使用）
    private volatile boolean stepRestorePending = false; // 正在从服务端恢复步数
    private Handler mainHandler;  // 主线程 Handler，传感器回调需要 Looper
    private static final String PREF_STEP_DAY_START = "step_day_start_counter";
    private static final String PREF_STEP_REBOOT_OFFSET = "step_reboot_offset";
    private static final String PREF_STEP_LAST_KNOWN = "step_last_known_counter";
    private static final String PREF_STEP_RESET_DATE = "step_reset_logical_date";
    private static final int STEP_RESET_HOUR = 5;  // 凌晨 5 点重置

    // ══════════════════════════════════════════════════════════
    //  生命周期
    // ══════════════════════════════════════════════════════════

    @Override
    public void onCreate() {
        super.onCreate();
        Log.i(TAG, "=== onCreate ===");
        createNotificationChannels();
        mainHandler = new Handler(Looper.getMainLooper());

        PowerManager pm = (PowerManager) getSystemService(Context.POWER_SERVICE);
        if (pm != null) {
            wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "AionChat:Push");
            wakeLock.acquire();
            Log.i(TAG, "WakeLock acquired");
        }

        WifiManager wm = (WifiManager) getApplicationContext().getSystemService(Context.WIFI_SERVICE);
        if (wm != null) {
            wifiLock = wm.createWifiLock(WifiManager.WIFI_MODE_FULL_LOW_LATENCY, "AionChat:Wifi");
            wifiLock.acquire();
            Log.i(TAG, "WifiLock acquired");
        }

        client = new OkHttpClient.Builder()
                .pingInterval(30, TimeUnit.SECONDS)
                .readTimeout(0, TimeUnit.SECONDS)
                .connectTimeout(10, TimeUnit.SECONDS)
                .build();

        registerNetworkCallback();
        initStepCounter();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null) {
            String action = intent.getStringExtra("action");
            if ("set_foreground".equals(action)) {
                isForegroundActive = intent.getBooleanExtra("active", false);
                if (isForegroundActive) stopMusic(); // WebView 接管，停止原生播放
                Log.d(TAG, "foreground=" + isForegroundActive);
                return START_STICKY;
            }
            if (ACTION_START_PHONE_SCREEN.equals(action)) {
                int resultCode = intent.getIntExtra(EXTRA_RESULT_CODE, 0);
                Intent resultData = intent.getParcelableExtra(EXTRA_RESULT_DATA);
                startPhoneScreenProjection(resultCode, resultData);
                // 不提前返回：如果这是 Service 首次启动，还需要继续初始化 URL、前台服务和 WebSocket。
            }
            if (ACTION_STOP_PHONE_SCREEN.equals(action)) {
                stopPhoneScreenProjection();
                return START_STICKY;
            }
            if (ACTION_TEST_ACCESSIBILITY_SCREEN.equals(action)) {
                requestAccessibilityPhoneScreen("manual_test", true);
                return START_STICKY;
            }

            String url = intent.getStringExtra("url");
            if (url != null) {
                String ws = url.replace("http://", "ws://").replace("https://", "wss://");
                if (!ws.endsWith("/ws")) {
                    ws = ws.replace("/chat", "/ws");
                    if (!ws.endsWith("/ws")) ws += "/ws";
                }
                if (ws.equals(serverUrl) && wsConnected.get()) {
                    Log.d(TAG, "Already connected to " + serverUrl);
                    return START_STICKY;
                }
                serverUrl = ws;
            }
        }

        if (serverUrl == null) {
            SharedPreferences prefs = getSharedPreferences("aion_prefs", MODE_PRIVATE);
            String saved = prefs.getString("saved_url", "http://192.168.xx.xxx:8080/chat");
            serverUrl = saved.replace("http://", "ws://").replace("https://", "wss://")
                             .replace("/chat", "/ws");
        }

        Log.i(TAG, "onStartCommand url=" + serverUrl);

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            // Android 14+: 需要声明所有用到的前台服务类型
            int serviceType = ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC;
            if (phoneScreenEnabled || mediaProjection != null) {
                serviceType |= ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PROJECTION;
            }
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION)
                    == PackageManager.PERMISSION_GRANTED) {
                serviceType |= ServiceInfo.FOREGROUND_SERVICE_TYPE_LOCATION;
            }
            startForeground(NOTIF_FOREGROUND, buildKeepAlive("连接中..."), serviceType);
        } else {
            startForeground(NOTIF_FOREGROUND, buildKeepAlive("连接中..."));
        }

        shouldRun = true;
        startHeartbeatThread();
        startLocationThread();
        startActivityThread();
        startRingSyncThread();
        return START_STICKY;
    }

    @Nullable @Override
    public IBinder onBind(Intent intent) { return null; }

    @Override
    public void onDestroy() {
        Log.i(TAG, "=== onDestroy ===");
        shouldRun = false;
        wsGeneration.incrementAndGet();
        if (heartbeatThread != null) heartbeatThread.interrupt();
        if (locationThread != null) locationThread.interrupt();
        if (activityThread != null) activityThread.interrupt();
        if (ringSyncThread != null) ringSyncThread.interrupt();
        if (ringBackgroundSync != null) ringBackgroundSync.close();
        stopEsp32Bridge();
        stopPhoneScreenProjection();
        unregisterScreenReceiver();
        if (sensorManager != null) sensorManager.unregisterListener(stepListener);
        if (webSocket != null) try { webSocket.cancel(); } catch (Exception ignored) {}
        if (client != null) client.dispatcher().executorService().shutdown();
        stopMusic();
        if (wakeLock != null && wakeLock.isHeld()) wakeLock.release();
        if (wifiLock != null && wifiLock.isHeld()) wifiLock.release();
        unregisterNetworkCallback();
        super.onDestroy();
    }

    @Override
    public void onTaskRemoved(Intent rootIntent) {
        Log.w(TAG, "Task removed → schedule restart");
        Intent ri = new Intent(getApplicationContext(), AionPushService.class);
        ri.setPackage(getPackageName());
        PendingIntent pi = PendingIntent.getService(getApplicationContext(), 1, ri,
                PendingIntent.FLAG_ONE_SHOT | PendingIntent.FLAG_IMMUTABLE);
        AlarmManager am = (AlarmManager) getSystemService(Context.ALARM_SERVICE);
        if (am != null) {
            am.setExactAndAllowWhileIdle(AlarmManager.ELAPSED_REALTIME_WAKEUP,
                    SystemClock.elapsedRealtime() + 3000, pi);
        }
        super.onTaskRemoved(rootIntent);
    }

    // ══════════════════════════════════════════════════════════
    //  网络变化监听 — 网络恢复时立即触发重连
    // ══════════════════════════════════════════════════════════

    private void registerNetworkCallback() {
        connectivityManager = (ConnectivityManager) getSystemService(Context.CONNECTIVITY_SERVICE);
        if (connectivityManager == null) return;

        networkCallback = new ConnectivityManager.NetworkCallback() {
            @Override
            public void onAvailable(Network network) {
                Log.i(TAG, "★ Network available, connected=" + wsConnected.get());
                if (!wsConnected.get() && shouldRun) {
                    reconnectDelay = 3000;
                    connectWebSocket();
                }
            }
            @Override
            public void onLost(Network network) {
                Log.w(TAG, "★ Network lost");
                wsConnected.set(false);
                updateKeepAlive("网络断开，等待恢复...");
            }
        };

        NetworkRequest req = new NetworkRequest.Builder()
                .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                .build();
        connectivityManager.registerNetworkCallback(req, networkCallback);
        Log.i(TAG, "NetworkCallback registered");
    }

    private void unregisterNetworkCallback() {
        if (connectivityManager != null && networkCallback != null) {
            try { connectivityManager.unregisterNetworkCallback(networkCallback); }
            catch (Exception ignored) {}
        }
    }

    // ══════════════════════════════════════════════════════════
    //  心跳线程 — 纯 Java Thread
    // ══════════════════════════════════════════════════════════

    private synchronized void startHeartbeatThread() {
        if (heartbeatThread != null && heartbeatThread.isAlive()) return;

        heartbeatThread = new Thread(() -> {
            Log.i(TAG, "♥ Heartbeat started tid=" + Thread.currentThread().getId());

            if (!wsConnected.get()) connectWebSocket();

            while (shouldRun) {
                try { Thread.sleep(HEARTBEAT_MS); }
                catch (InterruptedException e) { break; }
                if (!shouldRun) break;

                try {
                    if (wsConnected.get() && webSocket != null) {
                        boolean sent = webSocket.send("{\"type\":\"ping\"}");
                        long elapsed = (lastMessageTime > 0)
                                ? (System.currentTimeMillis() - lastMessageTime) / 1000 : 0;
                        Log.d(TAG, "♥ ping=" + sent + " msgs=" + msgReceived + " idle=" + elapsed + "s");

                        if (!sent) {
                            Log.w(TAG, "♥ ping failed → reconnect");
                            wsConnected.set(false);
                            connectWebSocket();
                        } else if (lastMessageTime > 0
                                && System.currentTimeMillis() - lastMessageTime > HEALTH_TIMEOUT) {
                            Log.w(TAG, "♥ health timeout → reconnect");
                            wsConnected.set(false);
                            connectWebSocket();
                        }
                    } else if (!wsConnected.get()) {
                        Log.i(TAG, "♥ not connected → reconnect");
                        connectWebSocket();
                    }
                } catch (Exception e) {
                    Log.e(TAG, "♥ error: " + e.getMessage());
                }
            }
            Log.i(TAG, "♥ Heartbeat exiting");
        }, "AionHeartbeat");
        heartbeatThread.setDaemon(false);
        heartbeatThread.start();
    }

    // ══════════════════════════════════════════════════════════
    //  定位上报线程 — 每隔 N 分钟获取 GPS 坐标并 POST 到服务器
    // ══════════════════════════════════════════════════════════

    private synchronized void startLocationThread() {
        if (locationThread != null && locationThread.isAlive()) return;

        locationThread = new Thread(() -> {
            Log.i(TAG, "📍 Location thread started");
            // 首次等 15 秒让 WS 和 GPS 稳定
            try { Thread.sleep(15000); } catch (InterruptedException e) { return; }

            while (shouldRun) {
                try {
                    // 权限可能在服务启动后才授予，重试初始化步数传感器
                    if (latestStepCounter < 0) initStepCounter();

                    // 先检查服务端定位功能是否启用
                    checkLocationEnabled();
                    if (locationEnabled) {
                        requestLocationOnce();
                    } else {
                        Log.d(TAG, "📍 server location disabled, idle");
                    }
                } catch (Exception e) {
                    Log.e(TAG, "📍 error: " + e.getMessage());
                }

                long interval = locationEnabled ? locationInterval : LOCATION_INTERVAL_DISABLED;
                try { Thread.sleep(interval); }
                catch (InterruptedException e) { break; }
            }
            Log.i(TAG, "📍 Location thread exiting");
        }, "AionLocation");
        locationThread.setDaemon(false);
        locationThread.start();
    }

    private void checkLocationEnabled() {
        if (serverUrl == null) return;
        String httpBase = serverUrl
                .replace("ws://", "http://")
                .replace("wss://", "https://")
                .replace("/ws", "");
        try {
            Request req = new Request.Builder()
                    .url(httpBase + "/api/location/config")
                    .get().build();
            try (Response resp = client.newCall(req).execute()) {
                if (resp.isSuccessful() && resp.body() != null) {
                    JSONObject cfg = new JSONObject(resp.body().string());
                    // active = enabled && 不在静默时段（服务端计算）
                    locationEnabled = cfg.optBoolean("active", false);
                }
            }
        } catch (Exception e) {
            Log.d(TAG, "📍 check config failed: " + e.getMessage());
        }
    }

    private void requestLocationOnce() {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION)
                != PackageManager.PERMISSION_GRANTED) {
            Log.w(TAG, "📍 No location permission");
            return;
        }

        if (locationManager == null) {
            locationManager = (LocationManager) getSystemService(Context.LOCATION_SERVICE);
        }
        if (locationManager == null) return;

        // 优先尝试 GPS，备用 Network
        Location loc = null;
        try {
            loc = locationManager.getLastKnownLocation(LocationManager.GPS_PROVIDER);
        } catch (Exception ignored) {}
        if (loc == null || System.currentTimeMillis() - loc.getTime() > 10 * 60_000) {
            try {
                loc = locationManager.getLastKnownLocation(LocationManager.NETWORK_PROVIDER);
            } catch (Exception ignored) {}
        }

        // 如果缓存的位置太旧（>10分钟），请求一次实时定位
        if (loc == null || System.currentTimeMillis() - loc.getTime() > 10 * 60_000) {
            requestFreshLocation();
            return;
        }

        lastKnownLocation = loc;
        postLocationToServer(loc);
    }

    private void requestFreshLocation() {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION)
                != PackageManager.PERMISSION_GRANTED) return;
        if (locationManager == null) return;

        // 注意: LocationListener 回调发生在 Looper 线程，这里用主线程 Looper
        try {
            String provider = locationManager.isProviderEnabled(LocationManager.GPS_PROVIDER)
                    ? LocationManager.GPS_PROVIDER : LocationManager.NETWORK_PROVIDER;

            locationManager.requestSingleUpdate(provider, new LocationListener() {
                @Override
                public void onLocationChanged(Location location) {
                    lastKnownLocation = location;
                    postLocationToServer(location);
                }
                @Override public void onStatusChanged(String p, int s, Bundle e) {}
                @Override public void onProviderEnabled(String p) {}
                @Override public void onProviderDisabled(String p) {}
            }, getMainLooper());
        } catch (Exception e) {
            Log.e(TAG, "📍 requestSingleUpdate failed: " + e.getMessage());
        }
    }

    private void postLocationToServer(Location loc) {
        if (loc == null || serverUrl == null) return;

        // 从 wsUrl 推断 HTTP API 地址
        String httpBase = serverUrl
                .replace("ws://", "http://")
                .replace("wss://", "https://")
                .replace("/ws", "");

        String apiUrl = httpBase + "/api/location/heartbeat";

        try {
            JSONObject body = new JSONObject();
            body.put("lng", loc.getLongitude());
            body.put("lat", loc.getLatitude());
            body.put("accuracy", loc.getAccuracy());
            body.put("is_gcj02", false);  // Android 原生 GPS 输出 WGS84

            // 搭载步数数据
            int steps = getTodaySteps();
            if (steps >= 0) {
                body.put("steps", steps);
                body.put("step_logical_date", getLogicalDate());
            }
            // 传感器诊断信息一并上报，方便服务端排查
            boolean hasPerm = ContextCompat.checkSelfPermission(this,
                    Manifest.permission.ACTIVITY_RECOGNITION) == PackageManager.PERMISSION_GRANTED;
            String stepDiag = "steps=" + steps
                    + " sensorVal=" + latestStepCounter
                    + " sensorObj=" + (stepSensor != null)
                    + " perm=" + hasPerm;
            body.put("step_diag", stepDiag);
            Log.i(TAG, "\uD83D\uDC63 " + stepDiag);

            MediaType JSON = MediaType.get("application/json; charset=utf-8");
            RequestBody reqBody = RequestBody.create(body.toString(), JSON);
            Request req = new Request.Builder().url(apiUrl).post(reqBody).build();

            // 同步请求（已在后台线程）
            try (Response resp = client.newCall(req).execute()) {
                String respBody = resp.body() != null ? resp.body().string() : "";
                Log.i(TAG, "📍 posted loc (" + String.format("%.4f,%.4f", loc.getLongitude(), loc.getLatitude())
                        + " acc=" + (int) loc.getAccuracy() + "m) → " + resp.code());
            }
        } catch (Exception e) {
            Log.e(TAG, "📍 post failed: " + e.getMessage());
        }
    }

    private synchronized void startRingSyncThread() {
        if (ringSyncThread != null && ringSyncThread.isAlive()) return;
        ringSyncThread = new Thread(() -> {
            Log.i(TAG, "💍 Ring sync thread started");
            while (shouldRun) {
                long delay = computeNextRingSyncDelayMs();
                Log.i(TAG, "💍 next background ring sync in " + (delay / 1000) + "s");
                try { Thread.sleep(delay); }
                catch (InterruptedException e) { break; }
                if (!shouldRun) break;
                try {
                    if (ringBackgroundSync == null) {
                        ringBackgroundSync = new RingBackgroundSync();
                    }
                    ringBackgroundSync.syncHeartHistoryOnce();
                } catch (Exception e) {
                    Log.e(TAG, "💍 sync failed: " + e.getMessage());
                    if (ringBackgroundSync != null) {
                        ringBackgroundSync.close();
                        ringBackgroundSync = null;
                    }
                }
            }
            if (ringBackgroundSync != null) {
                ringBackgroundSync.close();
                ringBackgroundSync = null;
            }
            Log.i(TAG, "💍 Ring sync thread exiting");
        }, "AionRingSync");
        ringSyncThread.setDaemon(false);
        ringSyncThread.start();
    }

    private long computeNextRingSyncDelayMs() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        int minute = cal.get(Calendar.MINUTE);
        int targetMinute = (minute / 10) * 10 + RING_SYNC_OFFSET_MINUTE;
        if (minute > targetMinute || (minute == targetMinute && Calendar.getInstance().get(Calendar.SECOND) > 0)) {
            targetMinute += 10;
        }
        if (targetMinute >= 60) {
            cal.add(Calendar.HOUR_OF_DAY, 1);
            targetMinute -= 60;
        }
        cal.set(Calendar.MINUTE, targetMinute);
        long delay = cal.getTimeInMillis() - System.currentTimeMillis();
        if (delay <= 0) delay += RING_SYNC_INTERVAL;
        if (delay < 1000) delay = 1000;
        return delay;
    }

    private class RingBackgroundSync {
        private static final String PREFS_NAME = "aion_ring_ble";
        private static final String KEY_DEVICE_ADDRESS = "device_address";
        private static final String KEY_DEVICE_NAME = "device_name";
        private static final String KEY_LAST_HEART_MEASURED_AT = "bg_last_heart_measured_at";
        private static final int DT_SETTING_TIME = 0x0100;
        private static final int DT_SETTING_HEART_MONITOR = 0x010C;
        private static final int DT_HEALTH_HEART = 0x0506;
        private static final int DT_HEALTH_HEART_ACK = 0x0515;
        private static final int DT_HEALTH_BLOCK = 0x0580;
        private static final int HEART_MONITOR_INTERVAL_MIN = 10;

        private final UUID cccdUuid = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb");
        private final RingBleService[] services = new RingBleService[] {
                new RingBleService("be940000-7333-be46-b7ae-689e71722bd5", "be940001-7333-be46-b7ae-689e71722bd5", "be940003-7333-be46-b7ae-689e71722bd5", "Main"),
                new RingBleService("6e400001-b5a3-f393-e0a9-e50e24dcca9e", "6e400002-b5a3-f393-e0a9-e50e24dcca9e", "6e400003-b5a3-f393-e0a9-e50e24dcca9e", "UART"),
                new RingBleService("0000ae00-0000-1000-8000-00805f9b34fb", "0000ae01-0000-1000-8000-00805f9b34fb", "0000ae02-0000-1000-8000-00805f9b34fb", "JieLi")
        };

        private BluetoothAdapter adapter;
        private BluetoothLeScanner scanner;
        private BluetoothGatt gatt;
        private BluetoothGattCharacteristic writeChar;
        private BluetoothDevice currentDevice;
        private CountDownLatch connectLatch;
        private CountDownLatch writeLatch;
        private CountDownLatch heartLatch;
        private final Object payloadLock = new Object();
        private final ArrayList<byte[]> heartPayloads = new ArrayList<>();
        private byte[] reassemblyData;
        private volatile boolean connected = false;
        private volatile boolean heartDone = false;
        private volatile BluetoothDevice scanMatch;
        private volatile CountDownLatch scanLatch;
        private String deviceName = "";

        RingBackgroundSync() {
            BluetoothManager bm = (BluetoothManager) getSystemService(Context.BLUETOOTH_SERVICE);
            if (bm != null) adapter = bm.getAdapter();
        }

        void syncHeartHistoryOnce() {
            String httpBase = getHttpBase();
            if (httpBase == null) {
                Log.d(TAG, "💍 no server url yet");
                return;
            }
            SharedPreferences prefs = getSharedPreferences(PREFS_NAME, MODE_PRIVATE);
            String savedAddress = prefs.getString(KEY_DEVICE_ADDRESS, "");
            String savedName = prefs.getString(KEY_DEVICE_NAME, "");
            if ((savedAddress == null || savedAddress.isEmpty()) && (savedName == null || savedName.isEmpty())) {
                Log.d(TAG, "💍 no saved ring device");
                postRingDiag(httpBase, "no_saved_device", "没有保存过戒指设备，后台无法自动同步", 0, 0, 0);
                return;
            }
            if (!hasBluetoothPermissionsForRing()) {
                Log.w(TAG, "💍 bluetooth permissions missing");
                postRingDiag(httpBase, "bluetooth_permission_missing", "蓝牙权限缺失，后台无法连接戒指", 0, 0, 0);
                return;
            }
            BluetoothDevice device = resolveSavedDevice(savedAddress, savedName);
            if (device == null) {
                Log.w(TAG, "💍 saved ring not found");
                postRingDiag(httpBase, "ring_not_found", "没有扫描到已保存戒指 savedName=" + savedName, 0, 0, 0);
                return;
            }
            try {
                connect(device);
                syncTimeAndMonitorSetting();
                ArrayList<HeartRateRecord> records = requestHeartHistory();
                if (records.isEmpty()) {
                    Log.i(TAG, "💍 no heart history records");
                    postRingDiag(httpBase, "no_records", "已连接戒指，但本次没有读到新的心率历史", 0, 0, 0);
                    return;
                }
                long lastUploaded = prefs.getLong(KEY_LAST_HEART_MEASURED_AT, 0);
                long newestUploaded = lastUploaded;
                int uploaded = 0;
                double newestMeasuredAt = 0;
                for (HeartRateRecord record : records) {
                    if (record.heartRate < 20 || record.heartRate > 240) continue;
                    long measuredMs = Math.round(record.measuredAt * 1000.0);
                    if (record.measuredAt > newestMeasuredAt) newestMeasuredAt = record.measuredAt;
                    if (measuredMs <= lastUploaded) continue;
                    if (postHeartRate(httpBase, record)) {
                        uploaded++;
                        if (measuredMs > newestUploaded) newestUploaded = measuredMs;
                    }
                }
                if (newestUploaded > lastUploaded) {
                    prefs.edit().putLong(KEY_LAST_HEART_MEASURED_AT, newestUploaded).apply();
                }
                Log.i(TAG, "💍 heart history synced, total=" + records.size() + ", uploaded=" + uploaded);
                postRingDiag(
                        httpBase,
                        uploaded > 0 ? "ok" : "no_new_records",
                        "后台心率同步完成 total=" + records.size() + ", uploaded=" + uploaded
                                + ", lastUploadedMs=" + lastUploaded,
                        records.size(),
                        uploaded,
                        newestMeasuredAt
                );
            } catch (Exception e) {
                postRingDiag(httpBase, "sync_failed", "后台心率同步失败：" + e.getMessage(), 0, 0, 0);
                throw e;
            } finally {
                close();
            }
        }

        private BluetoothDevice resolveSavedDevice(String savedAddress, String savedName) {
            if (adapter == null || !adapter.isEnabled()) return null;
            if (savedAddress != null && !savedAddress.isEmpty()) {
                try {
                    return adapter.getRemoteDevice(savedAddress);
                } catch (Exception e) {
                    Log.w(TAG, "💍 saved address invalid: " + e.getMessage());
                }
            }
            scanner = adapter.getBluetoothLeScanner();
            if (scanner == null) return null;
            scanMatch = null;
            scanLatch = new CountDownLatch(1);
            try {
                scanner.startScan(scanCallback);
                scanLatch.await(12, TimeUnit.SECONDS);
            } catch (Exception e) {
                Log.w(TAG, "💍 scan failed: " + e.getMessage());
            } finally {
                try { scanner.stopScan(scanCallback); } catch (Exception ignored) {}
            }
            return scanMatch;
        }

        private final ScanCallback scanCallback = new ScanCallback() {
            @Override
            public void onScanResult(int callbackType, ScanResult result) {
                BluetoothDevice dev = result.getDevice();
                String name = getDeviceName(dev);
                SharedPreferences prefs = getSharedPreferences(PREFS_NAME, MODE_PRIVATE);
                String savedName = prefs.getString(KEY_DEVICE_NAME, "");
                boolean matched = savedName != null && !savedName.isEmpty() && savedName.equals(name);
                if (!matched) matched = looksLikeRing(result, name);
                if (!matched) return;
                scanMatch = dev;
                CountDownLatch latch = scanLatch;
                if (latch != null) latch.countDown();
            }
        };

        private void connect(BluetoothDevice device) {
            close();
            currentDevice = device;
            deviceName = getDeviceName(device);
            connectLatch = new CountDownLatch(1);
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                gatt = device.connectGatt(AionPushService.this, false, gattCallback, BluetoothDevice.TRANSPORT_LE);
            } else {
                gatt = device.connectGatt(AionPushService.this, false, gattCallback);
            }
            try {
                if (!connectLatch.await(20, TimeUnit.SECONDS) || !connected || writeChar == null) {
                    throw new IllegalStateException("ring connect timeout");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("ring connect interrupted");
            }
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        private final BluetoothGattCallback gattCallback = new BluetoothGattCallback() {
            @Override
            public void onConnectionStateChange(BluetoothGatt g, int status, int newState) {
                if (status != BluetoothGatt.GATT_SUCCESS) {
                    connected = false;
                    CountDownLatch latch = connectLatch;
                    if (latch != null) latch.countDown();
                    return;
                }
                if (newState == BluetoothProfile.STATE_CONNECTED) {
                    try { g.requestConnectionPriority(BluetoothGatt.CONNECTION_PRIORITY_HIGH); } catch (Exception ignored) {}
                    mainHandler.postDelayed(g::discoverServices, 700);
                } else if (newState == BluetoothProfile.STATE_DISCONNECTED) {
                    connected = false;
                }
            }

            @Override
            public void onServicesDiscovered(BluetoothGatt g, int status) {
                if (status != BluetoothGatt.GATT_SUCCESS) {
                    CountDownLatch latch = connectLatch;
                    if (latch != null) latch.countDown();
                    return;
                }
                setupRingService(g);
            }

            @Override
            public void onCharacteristicChanged(BluetoothGatt g, BluetoothGattCharacteristic c) {
                processIncoming(c.getValue());
            }

            @Override
            public void onCharacteristicChanged(BluetoothGatt g, BluetoothGattCharacteristic c, byte[] value) {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                    processIncoming(value);
                }
            }

            @Override
            public void onCharacteristicWrite(BluetoothGatt g, BluetoothGattCharacteristic c, int status) {
                CountDownLatch latch = writeLatch;
                if (latch != null) latch.countDown();
            }

            @Override
            public void onDescriptorWrite(BluetoothGatt g, BluetoothGattDescriptor d, int status) {
                connected = true;
                CountDownLatch latch = connectLatch;
                if (latch != null) latch.countDown();
            }
        };

        private void setupRingService(BluetoothGatt g) {
            BluetoothGattService matchedService = null;
            RingBleService matched = null;
            for (RingBleService svc : services) {
                matchedService = g.getService(svc.service);
                if (matchedService != null) {
                    matched = svc;
                    break;
                }
            }
            if (matchedService == null || matched == null) {
                CountDownLatch latch = connectLatch;
                if (latch != null) latch.countDown();
                return;
            }
            writeChar = matchedService.getCharacteristic(matched.write);
            BluetoothGattCharacteristic notifyChar = matchedService.getCharacteristic(matched.notify);
            if (writeChar == null || notifyChar == null) {
                CountDownLatch latch = connectLatch;
                if (latch != null) latch.countDown();
                return;
            }
            if ((writeChar.getProperties() & BluetoothGattCharacteristic.PROPERTY_WRITE) != 0) {
                writeChar.setWriteType(BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT);
            } else {
                writeChar.setWriteType(BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE);
            }
            enableNotify(g, notifyChar);
            Log.i(TAG, "💍 background ring service ready: " + matched.name);
        }

        @SuppressWarnings("deprecation")
        private void enableNotify(BluetoothGatt g, BluetoothGattCharacteristic ch) {
            try {
                g.setCharacteristicNotification(ch, true);
                BluetoothGattDescriptor desc = ch.getDescriptor(cccdUuid);
                if (desc == null) {
                    connected = true;
                    CountDownLatch latch = connectLatch;
                    if (latch != null) latch.countDown();
                    return;
                }
                byte[] value = (ch.getProperties() & BluetoothGattCharacteristic.PROPERTY_INDICATE) != 0
                        ? BluetoothGattDescriptor.ENABLE_INDICATION_VALUE
                        : BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE;
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                    int result = g.writeDescriptor(desc, value);
                    if (result != android.bluetooth.BluetoothStatusCodes.SUCCESS) {
                        CountDownLatch latch = connectLatch;
                        if (latch != null) latch.countDown();
                    }
                } else {
                    desc.setValue(value);
                    if (!g.writeDescriptor(desc)) {
                        CountDownLatch latch = connectLatch;
                        if (latch != null) latch.countDown();
                    }
                }
            } catch (Exception e) {
                CountDownLatch latch = connectLatch;
                if (latch != null) latch.countDown();
            }
        }

        private void syncTimeAndMonitorSetting() {
            Calendar now = Calendar.getInstance();
            int dow = now.get(Calendar.DAY_OF_WEEK) - Calendar.MONDAY;
            if (dow < 0) dow = 6;
            byte[] timePayload = new byte[] {
                    (byte) (now.get(Calendar.YEAR) & 0xFF),
                    (byte) ((now.get(Calendar.YEAR) >> 8) & 0xFF),
                    (byte) (now.get(Calendar.MONTH) + 1),
                    (byte) now.get(Calendar.DAY_OF_MONTH),
                    (byte) now.get(Calendar.HOUR_OF_DAY),
                    (byte) now.get(Calendar.MINUTE),
                    (byte) now.get(Calendar.SECOND),
                    (byte) dow
            };
            writePacket(DT_SETTING_TIME, timePayload);
            sleepQuiet(350);
            writePacket(DT_SETTING_HEART_MONITOR, new byte[] {1, HEART_MONITOR_INTERVAL_MIN});
            sleepQuiet(350);
        }

        private ArrayList<HeartRateRecord> requestHeartHistory() {
            synchronized (payloadLock) {
                heartPayloads.clear();
                reassemblyData = null;
                heartDone = false;
            }
            heartLatch = new CountDownLatch(1);
            writePacket(DT_HEALTH_HEART, new byte[0]);
            try { heartLatch.await(35, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return parseHeartPayloads();
        }

        private void processIncoming(byte[] raw) {
            if (raw == null || raw.length < 4) return;
            byte[] packet;
            byte[] rest = null;
            synchronized (payloadLock) {
                if (reassemblyData != null) {
                    byte[] combined = new byte[reassemblyData.length + raw.length];
                    System.arraycopy(reassemblyData, 0, combined, 0, reassemblyData.length);
                    System.arraycopy(raw, 0, combined, reassemblyData.length, raw.length);
                    raw = combined;
                    reassemblyData = null;
                }
                int declaredLen = ((raw[2] & 0xFF) | ((raw[3] & 0xFF) << 8));
                if (declaredLen <= 0 || raw.length < declaredLen) {
                    reassemblyData = raw;
                    return;
                }
                packet = new byte[declaredLen];
                System.arraycopy(raw, 0, packet, 0, declaredLen);
                if (raw.length > declaredLen) {
                    rest = new byte[raw.length - declaredLen];
                    System.arraycopy(raw, declaredLen, rest, 0, rest.length);
                }
            }
            processPacket(packet);
            if (rest != null) processIncoming(rest);
        }

        private void processPacket(byte[] pkt) {
            if (pkt.length < 6) return;
            int dataType = ((pkt[0] & 0xFF) << 8) | (pkt[1] & 0xFF);
            int totalLen = (pkt[2] & 0xFF) | ((pkt[3] & 0xFF) << 8);
            int payloadLen = totalLen - 6;
            if (payloadLen < 0 || pkt.length < totalLen) return;
            byte[] payload = new byte[payloadLen];
            if (payloadLen > 0) System.arraycopy(pkt, 4, payload, 0, payloadLen);
            if (dataType == DT_HEALTH_HEART) {
                if (payload.length >= 2) {
                    int count = (payload[0] & 0xFF) | ((payload[1] & 0xFF) << 8);
                    if (count == 0) finishHeartHistory();
                }
                return;
            }
            if (dataType == DT_HEALTH_HEART_ACK) {
                if (payload.length == 0) {
                    finishHeartHistory();
                } else {
                    synchronized (payloadLock) { heartPayloads.add(payload); }
                }
                return;
            }
            if (dataType == DT_HEALTH_BLOCK) finishHeartHistory();
        }

        private void finishHeartHistory() {
            if (heartDone) return;
            heartDone = true;
            CountDownLatch latch = heartLatch;
            if (latch != null) latch.countDown();
        }

        private ArrayList<HeartRateRecord> parseHeartPayloads() {
            ArrayList<HeartRateRecord> records = new ArrayList<>();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            synchronized (payloadLock) {
                for (byte[] p : heartPayloads) out.write(p, 0, p.length);
            }
            byte[] payload = out.toByteArray();
            double offset = localEpoch2000Seconds();
            for (int i = 0; i + 6 <= payload.length; i += 6) {
                long ts = readU32LE(payload, i);
                int mode = payload[i + 4] & 0xFF;
                int hr = payload[i + 5] & 0xFF;
                records.add(new HeartRateRecord(offset + ts, hr, mode));
            }
            return records;
        }

        private boolean postHeartRate(String httpBase, HeartRateRecord record) {
            try {
                JSONObject raw = new JSONObject();
                raw.put("source", "android-background-history");
                raw.put("mode", record.mode);
                JSONObject body = new JSONObject();
                body.put("device_name", deviceName);
                body.put("heart_rate", record.heartRate);
                body.put("measured_at", record.measuredAt);
                body.put("source", "android-background-history");
                body.put("raw", raw);
                MediaType JSON = MediaType.get("application/json; charset=utf-8");
                RequestBody reqBody = RequestBody.create(body.toString(), JSON);
                Request req = new Request.Builder()
                        .url(httpBase + "/api/health/ring/heart-rate")
                        .post(reqBody)
                        .build();
                try (Response resp = client.newCall(req).execute()) {
                    Log.i(TAG, "💍 posted heart " + record.heartRate + " bpm → " + resp.code());
                    return resp.isSuccessful();
                }
            } catch (Exception e) {
                Log.e(TAG, "💍 post heart failed: " + e.getMessage());
                return false;
            }
        }

        private void postRingDiag(String httpBase, String status, String message, int total, int uploaded, double latestMeasuredAt) {
            if (httpBase == null) return;
            try {
                JSONObject body = new JSONObject();
                body.put("status", status);
                body.put("info", message
                        + "; total=" + total
                        + "; uploaded=" + uploaded
                        + "; latestMeasuredAt=" + latestMeasuredAt
                        + "; device=" + deviceName);
                MediaType JSON = MediaType.get("application/json; charset=utf-8");
                RequestBody reqBody = RequestBody.create(body.toString(), JSON);
                Request req = new Request.Builder()
                        .url(httpBase + "/api/health/ring/diag-report")
                        .post(reqBody)
                        .build();
                try (Response resp = client.newCall(req).execute()) {
                    Log.i(TAG, "💍 diag posted " + status + " → " + resp.code());
                }
            } catch (Exception e) {
                Log.e(TAG, "💍 diag post failed: " + e.getMessage());
            }
        }

        @SuppressWarnings("deprecation")
        private void writePacket(int dataType, byte[] payload) {
            if (gatt == null || writeChar == null) throw new IllegalStateException("ring not connected");
            byte[] pkt = buildPacket(dataType, payload);
            writeLatch = new CountDownLatch(1);
            boolean ok;
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                int result = gatt.writeCharacteristic(writeChar, pkt, writeChar.getWriteType());
                ok = result == android.bluetooth.BluetoothStatusCodes.SUCCESS;
            } else {
                writeChar.setValue(pkt);
                ok = gatt.writeCharacteristic(writeChar);
            }
            if (!ok) throw new IllegalStateException("write failed 0x" + Integer.toHexString(dataType));
            try { writeLatch.await(3, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        private byte[] buildPacket(int dataType, byte[] payload) {
            int payloadLen = payload == null ? 0 : payload.length;
            int totalLen = payloadLen + 6;
            byte[] pkt = new byte[totalLen];
            pkt[0] = (byte) ((dataType >> 8) & 0xFF);
            pkt[1] = (byte) (dataType & 0xFF);
            pkt[2] = (byte) (totalLen & 0xFF);
            pkt[3] = (byte) ((totalLen >> 8) & 0xFF);
            if (payloadLen > 0) System.arraycopy(payload, 0, pkt, 4, payloadLen);
            int crc = crc16(pkt, totalLen - 2);
            pkt[totalLen - 2] = (byte) (crc & 0xFF);
            pkt[totalLen - 1] = (byte) ((crc >> 8) & 0xFF);
            return pkt;
        }

        private int crc16(byte[] data, int len) {
            int crc = 0xFFFF;
            for (int i = 0; i < len; i++) {
                crc = ((crc << 8) & 0xFF00) | ((crc >> 8) & 0x00FF);
                crc ^= data[i] & 0xFF;
                crc ^= (crc & 0xFF) >> 4;
                crc ^= (crc << 12) & 0xFFFF;
                crc ^= ((crc & 0xFF) << 5) & 0xFFFF;
                crc &= 0xFFFF;
            }
            return crc;
        }

        private long readU32LE(byte[] arr, int offset) {
            return ((long) arr[offset] & 0xFF)
                    | (((long) arr[offset + 1] & 0xFF) << 8)
                    | (((long) arr[offset + 2] & 0xFF) << 16)
                    | (((long) arr[offset + 3] & 0xFF) << 24);
        }

        private double localEpoch2000Seconds() {
            Calendar cal = Calendar.getInstance();
            cal.clear();
            cal.set(2000, Calendar.JANUARY, 1, 0, 0, 0);
            return cal.getTimeInMillis() / 1000.0;
        }

        private boolean hasBluetoothPermissionsForRing() {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                return ContextCompat.checkSelfPermission(AionPushService.this, Manifest.permission.BLUETOOTH_SCAN) == PackageManager.PERMISSION_GRANTED
                        && ContextCompat.checkSelfPermission(AionPushService.this, Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED;
            }
            return true;
        }

        private boolean looksLikeRing(ScanResult result, String name) {
            if (name != null) {
                String upper = name.toUpperCase(Locale.US);
                if (upper.contains("SMART RING") || upper.contains("YCBT") || upper.startsWith("DFU")) return true;
            }
            ScanRecord record = result.getScanRecord();
            if (record == null || record.getServiceUuids() == null) return false;
            for (android.os.ParcelUuid uuid : record.getServiceUuids()) {
                for (RingBleService svc : services) {
                    if (svc.service.equals(uuid.getUuid())) return true;
                }
            }
            return false;
        }

        private String getDeviceName(BluetoothDevice dev) {
            try {
                String name = dev.getName();
                return name == null ? "" : name;
            } catch (Exception e) {
                return "";
            }
        }

        private void sleepQuiet(long ms) {
            try { Thread.sleep(ms); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        void close() {
            connected = false;
            try {
                if (gatt != null) {
                    gatt.disconnect();
                    gatt.close();
                }
            } catch (Exception ignored) {}
            gatt = null;
            writeChar = null;
            currentDevice = null;
        }

        private class RingBleService {
            final UUID service;
            final UUID write;
            final UUID notify;
            final String name;
            RingBleService(String service, String write, String notify, String name) {
                this.service = UUID.fromString(service);
                this.write = UUID.fromString(write);
                this.notify = UUID.fromString(notify);
                this.name = name;
            }
        }

        private class HeartRateRecord {
            final double measuredAt;
            final int heartRate;
            final int mode;
            HeartRateRecord(double measuredAt, int heartRate, int mode) {
                this.measuredAt = measuredAt;
                this.heartRate = heartRate;
                this.mode = mode;
            }
        }
    }

    // ══════════════════════════════════════════════════════════
    //  WebSocket 连接 — synchronized 防并发
    // ══════════════════════════════════════════════════════════

    private synchronized void connectWebSocket() {
        if (wsConnected.get()) return;
        if (serverUrl == null) { Log.e(TAG, "url=null"); return; }

        final int gen = wsGeneration.incrementAndGet();

        WebSocket old = webSocket;
        webSocket = null;
        if (old != null) try { old.cancel(); } catch (Exception ignored) {}

        Log.i(TAG, ">>> connect gen=" + gen + " → " + serverUrl);
        updateKeepAlive("连接中...");

        try {
            Request req = new Request.Builder().url(serverUrl).build();
            webSocket = client.newWebSocket(req, new WebSocketListener() {

                @Override
                public void onOpen(WebSocket ws, Response resp) {
                    if (gen != wsGeneration.get()) { ws.cancel(); return; }
                    Log.i(TAG, ">>> OPEN gen=" + gen);
                    wsConnected.set(true);
                    reconnectDelay = 3000;
                    msgReceived = 0;
                    lastMessageTime = System.currentTimeMillis();
                    updateKeepAlive("在线 ✨");
                }

                @Override
                public void onMessage(WebSocket ws, String text) {
                    if (gen != wsGeneration.get()) return;
                    lastMessageTime = System.currentTimeMillis();
                    handleMessage(text);
                }

                @Override
                public void onFailure(WebSocket ws, Throwable t, Response resp) {
                    if (gen != wsGeneration.get()) return;
                    String err = t != null ? t.getMessage() : "unknown";
                    Log.w(TAG, ">>> FAIL gen=" + gen + ": " + err);
                    wsConnected.set(false);
                    reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY);
                    updateKeepAlive("连接失败: " + err);
                    // 不在这里阻塞或重连！心跳线程会处理
                }

                @Override
                public void onClosed(WebSocket ws, int code, String reason) {
                    if (gen != wsGeneration.get()) return;
                    Log.i(TAG, ">>> CLOSED gen=" + gen + " code=" + code);
                    wsConnected.set(false);
                    updateKeepAlive("连接关闭(" + code + ")");
                }
            });
        } catch (Exception e) {
            Log.e(TAG, "connect error: " + e.getMessage());
            reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY);
        }
    }

    // ══════════════════════════════════════════════════════════
    //  消息 → 通知
    // ══════════════════════════════════════════════════════════

    private void handleMessage(String text) {
        try {
            JSONObject json = new JSONObject(text);
            String type = json.optString("type", "");

            if ("pong".equals(type) || "ping".equals(type)) return;

            msgReceived++;
            Log.d(TAG, "MSG #" + msgReceived + " type=" + type);

            JSONObject data = json.optJSONObject("data");

            switch (type) {
                case "schedule_alarm": {
                    String c = data != null ? data.optString("content", "闹铃") : "闹铃";
                    showNotif(CH_ALARM, "⏰ 闹铃", c, true);
                    break;
                }
                case "monitor_alert": {
                    String c = data != null ? data.optString("content", "监控提醒") : "监控提醒";
                    showNotif(CH_ALARM, "👁 监控", c, true);
                    schedulePhoneScreenCapture("monitor_alert");
                    break;
                }
                case "cam_check": {
                    schedulePhoneScreenCapture("cam_check");
                    break;
                }
                case "music": {
                    // 后台自动播放音乐（前台由 WebView JS 处理）
                    if (!isForegroundActive && data != null) {
                        JSONArray cards = data.optJSONArray("cards");
                        if (cards != null && cards.length() > 0) {
                            JSONObject firstCard = cards.optJSONObject(0);
                            if (firstCard != null) {
                                int songId = firstCard.optInt("id", 0);
                                if (songId > 0) {
                                    playMusicStream(songId);
                                }
                            }
                        }
                    }
                    break;
                }
                case "msg_created": {
                    if (data != null) {
                        String role = data.optString("role", "");
                        if ("assistant".equals(role)) {
                            String c = data.optString("content", "");
                            if (c.length() > 100) c = c.substring(0, 100) + "...";
                            String sender = data.optString("sender", "Aion");
                            if (sender.isEmpty()) sender = "Aion";
                            else sender = sender.substring(0, 1).toUpperCase() + sender.substring(1);
                            showNotif(CH_ALARM, "💬 " + sender, c, true);
                        }
                    }
                    break;
                }
                case "chatroom_msg_created": {
                    if (data != null) {
                        String sender = data.optString("sender", "");
                        if (!"user".equals(sender) && !"system".equals(sender) && !sender.isEmpty()) {
                            String c = data.optString("content", "");
                            if (c.length() > 100) c = c.substring(0, 100) + "...";
                            sender = sender.substring(0, 1).toUpperCase() + sender.substring(1);
                            showNotif(CH_ALARM, "💬 " + sender, c, true);
                        }
                    }
                    break;
                }
                case "esp32_bridge": {
                    if (data != null) {
                        boolean active = data.optBoolean("active", false);
                        if (active) {
                            String captureUrl = data.optString("url", "");
                            if (!captureUrl.isEmpty()) {
                                startEsp32Bridge(captureUrl);
                            }
                        } else {
                            stopEsp32Bridge();
                        }
                    }
                    break;
                }
                case "request_location_sync": {
                    // 服务端请求立即上报位置+步数
                    Log.i(TAG, "📍 Force sync requested via WS");
                    new Thread(() -> {
                        try {
                            if (latestStepCounter < 0) initStepCounter();
                            requestLocationOnce();
                        } catch (Exception e) {
                            Log.e(TAG, "📍 Force sync error: " + e.getMessage());
                        }
                    }, "ForceSyncLocation").start();
                    break;
                }
                case "request_step_diag": {
                    // 诊断步数传感器状态，在主线程执行
                    mainHandler.post(() -> {
                        try {
                            boolean hasPerm = ContextCompat.checkSelfPermission(
                                    AionPushService.this,
                                    Manifest.permission.ACTIVITY_RECOGNITION)
                                    == PackageManager.PERMISSION_GRANTED;
                            SharedPreferences dp = getSharedPreferences("aion_prefs", MODE_PRIVATE);
                            String diagInfo = "perm=" + hasPerm
                                    + " sensorObj=" + (stepSensor != null)
                                    + " latestVal=" + latestStepCounter
                                    + " dayStart=" + dp.getFloat(PREF_STEP_DAY_START, -1)
                                    + " offset=" + dp.getFloat(PREF_STEP_REBOOT_OFFSET, 0)
                                    + " lastKnown=" + dp.getFloat(PREF_STEP_LAST_KNOWN, -1)
                                    + " resetDate=" + dp.getString(PREF_STEP_RESET_DATE, "")
                                    + " todaySteps=" + getTodaySteps();
                            Log.i(TAG, "\uD83D\uDC63 DIAG: " + diagInfo);
                            // 尝试重新初始化
                            if (stepSensor == null) initStepCounter();
                            // 通过 HTTP POST 发给服务端（不走 WS，更可靠）
                            String httpBase = serverUrl
                                    .replace("ws://", "http://")
                                    .replace("wss://", "https://")
                                    .replace("/ws", "");
                            new Thread(() -> {
                                try {
                                    JSONObject body = new JSONObject();
                                    body.put("info", diagInfo);
                                    MediaType JSON_T = MediaType.get("application/json; charset=utf-8");
                                    RequestBody reqBody = RequestBody.create(body.toString(), JSON_T);
                                    Request req = new Request.Builder()
                                            .url(httpBase + "/api/location/step-diag-report")
                                            .post(reqBody).build();
                                    try (Response resp = client.newCall(req).execute()) {
                                        Log.i(TAG, "\uD83D\uDC63 diag posted: " + resp.code());
                                    }
                                } catch (Exception e) {
                                    Log.e(TAG, "\uD83D\uDC63 diag post failed: " + e.getMessage());
                                }
                            }, "StepDiag").start();
                        } catch (Exception e) {
                            Log.e(TAG, "\uD83D\uDC63 diag error: " + e.getMessage());
                        }
                    });
                    break;
                }
                case "request_ring_diag": {
                    SharedPreferences rp = getSharedPreferences("aion_ring_ble", MODE_PRIVATE);
                    boolean hasPerm = hasBluetoothPermissionsForRing();
                    boolean btEnabled = false;
                    try {
                        BluetoothManager bm = (BluetoothManager) getSystemService(Context.BLUETOOTH_SERVICE);
                        BluetoothAdapter ad = bm != null ? bm.getAdapter() : null;
                        btEnabled = ad != null && ad.isEnabled();
                    } catch (Exception ignored) {}
                    String info = "bluetoothEnabled=" + btEnabled
                            + "; bluetoothPerm=" + hasPerm
                            + "; ringThreadAlive=" + (ringSyncThread != null && ringSyncThread.isAlive())
                            + "; savedName=" + rp.getString("device_name", "")
                            + "; savedAddress=" + rp.getString("device_address", "")
                            + "; lastUploadedMs=" + rp.getLong("bg_last_heart_measured_at", 0)
                            + "; wsConnected=" + wsConnected.get()
                            + "; foreground=" + isForegroundActive;
                    Log.i(TAG, "💍 DIAG: " + info);
                    postRingDiagNow("diag", info);
                    break;
                }
            }
        } catch (Exception e) {
            Log.w(TAG, "parse error: " + e.getMessage());
        }
    }

    private boolean hasBluetoothPermissionsForRing() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            return ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_SCAN) == PackageManager.PERMISSION_GRANTED
                    && ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED;
        }
        return true;
    }

    private void postRingDiagNow(String status, String info) {
        String httpBase = getHttpBase();
        if (httpBase == null) return;
        new Thread(() -> {
            try {
                JSONObject body = new JSONObject();
                body.put("status", status);
                body.put("info", info);
                MediaType JSON_T = MediaType.get("application/json; charset=utf-8");
                RequestBody reqBody = RequestBody.create(body.toString(), JSON_T);
                Request req = new Request.Builder()
                        .url(httpBase + "/api/health/ring/diag-report")
                        .post(reqBody)
                        .build();
                try (Response resp = client.newCall(req).execute()) {
                    Log.i(TAG, "💍 diag now posted: " + resp.code());
                }
            } catch (Exception e) {
                Log.e(TAG, "💍 diag now post failed: " + e.getMessage());
            }
        }, "RingDiag").start();
    }

    // ══════════════════════════════════════════════════════════
    //  ESP32-CAM 桥接（手机从 ESP32 拉帧 → 上传服务器）
    // ══════════════════════════════════════════════════════════

    private void startEsp32Bridge(String captureUrl) {
        stopEsp32Bridge();
        esp32CaptureUrl = captureUrl;
        esp32BridgeActive = true;
        esp32BridgeThread = new Thread(() -> {
            Log.i(TAG, "📷 ESP32 bridge started: " + captureUrl);
            int failCount = 0;
            while (esp32BridgeActive && shouldRun) {
                try {
                    // 从 ESP32 拉一帧 JPEG
                    Request req = new Request.Builder()
                            .url(esp32CaptureUrl)
                            .get()
                            .build();
                    byte[] jpgBytes;
                    try (Response resp = client.newCall(req).execute()) {
                        if (!resp.isSuccessful() || resp.body() == null) {
                            failCount++;
                            if (failCount % 10 == 0) {
                                Log.w(TAG, "📷 ESP32 fetch failed " + failCount + " times");
                            }
                            Thread.sleep(Math.min(5000, 1000 + failCount * 500L));
                            continue;
                        }
                        jpgBytes = resp.body().bytes();
                    }
                    if (jpgBytes.length < 100) {
                        failCount++;
                        Thread.sleep(1000);
                        continue;
                    }

                    // 上传到服务器
                    String httpBase = serverUrl
                            .replace("ws://", "http://")
                            .replace("wss://", "https://")
                            .replace("/ws", "");
                    RequestBody body = RequestBody.create(jpgBytes,
                            MediaType.get("image/jpeg"));
                    Request upload = new Request.Builder()
                            .url(httpBase + "/api/cam/esp32/frame")
                            .post(body)
                            .build();
                    try (Response uploadResp = client.newCall(upload).execute()) {
                        if (uploadResp.isSuccessful()) {
                            failCount = 0;
                        } else {
                            failCount++;
                        }
                    }
                    // 正常 ~1fps
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    failCount++;
                    if (failCount % 10 == 0) {
                        Log.e(TAG, "📷 ESP32 bridge error: " + e.getMessage());
                    }
                    try { Thread.sleep(Math.min(5000, 1000 + failCount * 500L)); }
                    catch (InterruptedException ie) { break; }
                }
            }
            Log.i(TAG, "📷 ESP32 bridge stopped");
        }, "Esp32Bridge");
        esp32BridgeThread.setDaemon(true);
        esp32BridgeThread.start();
    }

    private void stopEsp32Bridge() {
        esp32BridgeActive = false;
        if (esp32BridgeThread != null && esp32BridgeThread.isAlive()) {
            esp32BridgeThread.interrupt();
            try { esp32BridgeThread.join(3000); } catch (InterruptedException ignored) {}
        }
        esp32BridgeThread = null;
    }

    // ══════════════════════════════════════════════════════════
    //  原生音乐播放（后台 WebView 冻结时由 MediaPlayer 接管）
    // ══════════════════════════════════════════════════════════

    private void playMusicStream(int songId) {
        // ws://host:port/ws → http://host:port
        String httpBase = serverUrl.replace("ws://", "http://").replace("wss://", "https://");
        if (httpBase.endsWith("/ws")) httpBase = httpBase.substring(0, httpBase.length() - 3);
        String streamUrl = httpBase + "/api/music/stream/" + songId;
        Log.i(TAG, "♪ Playing music: " + streamUrl);

        stopMusic();

        try {
            mediaPlayer = new MediaPlayer();
            mediaPlayer.setAudioAttributes(new AudioAttributes.Builder()
                    .setContentType(AudioAttributes.CONTENT_TYPE_MUSIC)
                    .setUsage(AudioAttributes.USAGE_ALARM)  // 走闹钟音频流，可穿透勿扰模式
                    .build());
            mediaPlayer.setDataSource(streamUrl);
            mediaPlayer.setOnPreparedListener(MediaPlayer::start);
            mediaPlayer.setOnCompletionListener(mp -> {
                Log.i(TAG, "♪ Music finished");
                mp.release();
                if (mediaPlayer == mp) mediaPlayer = null;
            });
            mediaPlayer.setOnErrorListener((mp, what, extra) -> {
                Log.e(TAG, "♪ MediaPlayer error: " + what + "/" + extra);
                mp.release();
                if (mediaPlayer == mp) mediaPlayer = null;
                return true;
            });
            mediaPlayer.prepareAsync();
        } catch (Exception e) {
            Log.e(TAG, "♪ Music play error: " + e.getMessage());
            if (mediaPlayer != null) {
                try { mediaPlayer.release(); } catch (Exception ignored) {}
                mediaPlayer = null;
            }
        }
    }

    private void stopMusic() {
        if (mediaPlayer != null) {
            try {
                if (mediaPlayer.isPlaying()) mediaPlayer.stop();
                mediaPlayer.release();
            } catch (Exception ignored) {}
            mediaPlayer = null;
        }
    }

    private void showNotif(String ch, String title, String text, boolean high) {
        NotificationManager nm = getSystemService(NotificationManager.class);
        if (nm == null) return;

        Log.i(TAG, "NOTIFY " + title + ": " + text);

        Intent i = new Intent(this, LauncherActivity.class);
        i.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);
        PendingIntent pi = PendingIntent.getActivity(this, notifCounter, i,
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

        NotificationCompat.Builder b = new NotificationCompat.Builder(this, ch)
                .setSmallIcon(R.mipmap.ic_launcher)
                .setContentTitle(title)
                .setContentText(text)
                .setStyle(new NotificationCompat.BigTextStyle().bigText(text))
                .setPriority(high ? NotificationCompat.PRIORITY_HIGH : NotificationCompat.PRIORITY_DEFAULT)
                .setContentIntent(pi)
                .setAutoCancel(true)
                .setCategory(high ? NotificationCompat.CATEGORY_ALARM : NotificationCompat.CATEGORY_MESSAGE)
                .setVisibility(NotificationCompat.VISIBILITY_PUBLIC);

        if (high) {
            b.setDefaults(NotificationCompat.DEFAULT_ALL);
            b.setFullScreenIntent(pi, true);  // 锁屏时亮屏弹出
        }

        nm.notify(NOTIF_MSG_BASE + (notifCounter++ % 50), b.build());
    }

    // ══════════════════════════════════════════════════════════
    //  手机屏幕截图 — MediaProjection 授权后按监控提示抓取一帧
    // ══════════════════════════════════════════════════════════

    private void startPhoneScreenProjection(int resultCode, Intent resultData) {
        if (resultCode == 0 || resultData == null) {
            Log.w(TAG, "📱 screen projection missing result");
            return;
        }
        synchronized (phoneScreenLock) {
            stopPhoneScreenProjectionLocked();
            try {
                if (projectionManager == null) {
                    projectionManager = (MediaProjectionManager) getSystemService(Context.MEDIA_PROJECTION_SERVICE);
                }
                if (projectionManager == null) {
                    Log.w(TAG, "📱 MediaProjectionManager unavailable");
                    postPhoneScreenSkip("projection_manager_unavailable", false);
                    return;
                }

                // Android 14+ 要求 MediaProjection 会话运行在 mediaProjection 类型的前台服务中。
                // 用户授权已在 ActivityResult 中完成，这里先把服务类型提升，再创建投影实例和虚拟显示。
                updateForegroundForProjection();
                mediaProjection = projectionManager.getMediaProjection(resultCode, resultData);
                if (mediaProjection == null) {
                    Log.w(TAG, "📱 MediaProjection unavailable");
                    postPhoneScreenSkip("projection_unavailable", false);
                    return;
                }
                mediaProjection.registerCallback(new MediaProjection.Callback() {
                    @Override
                    public void onStop() {
                        synchronized (phoneScreenLock) {
                            stopPhoneScreenProjectionLocked();
                        }
                    }
                }, mainHandler);

                DisplayMetrics dm = getResources().getDisplayMetrics();
                int rawW = Math.max(1, dm.widthPixels);
                int rawH = Math.max(1, dm.heightPixels);
                float scale = Math.min(1f, 1080f / Math.max(rawW, rawH));
                int capW = Math.max(1, Math.round(rawW * scale));
                int capH = Math.max(1, Math.round(rawH * scale));

                phoneScreenReader = ImageReader.newInstance(capW, capH, PixelFormat.RGBA_8888, 2);
                phoneScreenDisplay = mediaProjection.createVirtualDisplay(
                        "AionPhoneScreen",
                        capW,
                        capH,
                        dm.densityDpi,
                        DisplayManager.VIRTUAL_DISPLAY_FLAG_AUTO_MIRROR,
                        phoneScreenReader.getSurface(),
                        null,
                        mainHandler
                );
                phoneScreenEnabled = true;
                Log.i(TAG, "📱 phone screen projection ready " + capW + "x" + capH);
            } catch (Exception e) {
                Log.e(TAG, "📱 start projection failed: " + e.getMessage());
                postPhoneScreenSkip("projection_start_failed:" + e.getClass().getSimpleName(), false);
                stopPhoneScreenProjectionLocked();
            }
        }
    }

    private void stopPhoneScreenProjection() {
        synchronized (phoneScreenLock) {
            stopPhoneScreenProjectionLocked();
        }
    }

    private void stopPhoneScreenProjectionLocked() {
        phoneScreenEnabled = false;
        if (phoneScreenDisplay != null) {
            try { phoneScreenDisplay.release(); } catch (Exception ignored) {}
            phoneScreenDisplay = null;
        }
        if (phoneScreenReader != null) {
            try { phoneScreenReader.close(); } catch (Exception ignored) {}
            phoneScreenReader = null;
        }
        if (mediaProjection != null) {
            MediaProjection oldProjection = mediaProjection;
            mediaProjection = null;
            try { oldProjection.stop(); } catch (Exception ignored) {}
        }
    }

    private void updateForegroundForProjection() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            int serviceType = ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
                    | ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PROJECTION;
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION)
                    == PackageManager.PERMISSION_GRANTED) {
                serviceType |= ServiceInfo.FOREGROUND_SERVICE_TYPE_LOCATION;
            }
            startForeground(NOTIF_FOREGROUND, buildKeepAlive("在线 ✨ · 手机屏幕监督已开启"), serviceType);
        } else {
            startForeground(NOTIF_FOREGROUND, buildKeepAlive("在线 ✨ · 手机屏幕监督已开启"));
        }
    }

    private void schedulePhoneScreenCapture(String reason) {
        schedulePhoneScreenSnapshot(reason, 4200, true);
    }

    private void schedulePhoneScreenSnapshot(String reason, long delayMs, boolean forceAccessibilityFallback) {
        if (System.currentTimeMillis() - lastPhoneCaptureAt < 3000) return;
        lastPhoneCaptureAt = System.currentTimeMillis();
        new Thread(() -> {
            try { Thread.sleep(Math.max(0, delayMs)); } catch (InterruptedException ignored) {}
            captureAndUploadPhoneScreen(reason, forceAccessibilityFallback);
        }, "PhoneScreenCapture").start();
    }

    private boolean isPhoneUnlockedForCapture() {
        try {
            PowerManager pm = (PowerManager) getSystemService(Context.POWER_SERVICE);
            if (pm != null && Build.VERSION.SDK_INT >= Build.VERSION_CODES.KITKAT_WATCH && !pm.isInteractive()) {
                return false;
            }
            KeyguardManager kg = (KeyguardManager) getSystemService(Context.KEYGUARD_SERVICE);
            if (kg != null) {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M && kg.isDeviceLocked()) return false;
                if (kg.isKeyguardLocked()) return false;
            }
        } catch (Exception e) {
            Log.w(TAG, "📱 lock state check failed: " + e.getMessage());
            return false;
        }
        return screenOn;
    }

    private boolean requestAccessibilityPhoneScreen(String reason, boolean force) {
        boolean enabledInSettings = AionAccessibilityService.isEnabledInSettings(this);
        String httpBase = getHttpBase();
        boolean accepted = AionAccessibilityService.captureLatest(
                this,
                lastReportedApp,
                reason,
                force,
                force ? 500 : 900,
                httpBase
        );
        Log.i(TAG, "📱 accessibility capture request reason=" + reason
                + " enabled=" + enabledInSettings
                + " accepted=" + accepted
                + " httpBase=" + httpBase
                + " app=" + lastReportedApp);
        if (!accepted) {
            postPhoneScreenSkip(enabledInSettings
                    ? "accessibility_not_connected"
                    : "accessibility_not_enabled", false);
        }
        return accepted;
    }

    private void captureAndUploadPhoneScreen(String reason, boolean forceAccessibilityFallback) {
        if (!phoneScreenEnabled || phoneScreenReader == null) {
            requestAccessibilityPhoneScreen("fallback_" + reason, forceAccessibilityFallback);
            return;
        }
        if (!isPhoneUnlockedForCapture()) {
            postPhoneScreenSkip("locked", true);
            return;
        }

        Image image = null;
        Bitmap bitmap = null;
        Bitmap cropped = null;
        Bitmap scaled = null;
        try {
            synchronized (phoneScreenLock) {
                if (phoneScreenReader == null) return;
                image = phoneScreenReader.acquireLatestImage();
            }
            if (image == null) {
                try { Thread.sleep(250); } catch (InterruptedException ignored) {}
                synchronized (phoneScreenLock) {
                    if (phoneScreenReader == null) return;
                    image = phoneScreenReader.acquireLatestImage();
                }
            }
            if (image == null) {
                postPhoneScreenSkip("no_frame", false);
                return;
            }

            int width = image.getWidth();
            int height = image.getHeight();
            Image.Plane plane = image.getPlanes()[0];
            ByteBuffer buffer = plane.getBuffer();
            int pixelStride = plane.getPixelStride();
            int rowStride = plane.getRowStride();
            int rowPadding = rowStride - pixelStride * width;
            int paddedWidth = width + rowPadding / pixelStride;

            bitmap = Bitmap.createBitmap(paddedWidth, height, Bitmap.Config.ARGB_8888);
            bitmap.copyPixelsFromBuffer(buffer);
            cropped = Bitmap.createBitmap(bitmap, 0, 0, width, height);

            float scale = Math.min(1f, 1080f / Math.max(width, height));
            if (scale < 1f) {
                int sw = Math.max(1, Math.round(width * scale));
                int sh = Math.max(1, Math.round(height * scale));
                scaled = Bitmap.createScaledBitmap(cropped, sw, sh, true);
            } else {
                scaled = cropped;
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            scaled.compress(Bitmap.CompressFormat.JPEG, 82, out);
            String b64 = Base64.encodeToString(out.toByteArray(), Base64.NO_WRAP);
            uploadPhoneScreenBase64(b64, reason);
        } catch (Exception e) {
            Log.e(TAG, "📱 capture failed: " + e.getMessage());
            if (!requestAccessibilityPhoneScreen("fallback_capture_failed_" + reason, forceAccessibilityFallback)) {
                postPhoneScreenSkip("capture_failed", false);
            }
        } finally {
            if (image != null) try { image.close(); } catch (Exception ignored) {}
            if (bitmap != null) bitmap.recycle();
            if (cropped != null && cropped != scaled) cropped.recycle();
            if (scaled != null) scaled.recycle();
        }
    }

    private String getHttpBase() {
        if (serverUrl == null) return null;
        return serverUrl.replace("ws://", "http://")
                .replace("wss://", "https://")
                .replace("/ws", "");
    }

    private void uploadPhoneScreenBase64(String b64, String reason) {
        String httpBase = getHttpBase();
        if (httpBase == null) return;
        try {
            JSONObject body = new JSONObject();
            body.put("image_base64", b64);
            body.put("timestamp", System.currentTimeMillis() / 1000.0);
            body.put("app", lastReportedApp);
            body.put("locked", false);
            body.put("reason", reason);
            body.put("source", "mediaprojection");
            MediaType JSON_TYPE = MediaType.get("application/json; charset=utf-8");
            RequestBody reqBody = RequestBody.create(body.toString(), JSON_TYPE);
            Request req = new Request.Builder()
                    .url(httpBase + "/api/phone-screen/upload")
                    .post(reqBody)
                    .build();
            try (Response resp = client.newCall(req).execute()) {
                Log.i(TAG, "📱 phone screen uploaded → " + resp.code());
            }
        } catch (Exception e) {
            Log.e(TAG, "📱 phone screen upload failed: " + e.getMessage());
        }
    }

    private void postPhoneScreenSkip(String reason, boolean locked) {
        new Thread(() -> postPhoneScreenSkipOnBackground(reason, locked), "PhoneScreenSkip").start();
    }

    private void postPhoneScreenSkipOnBackground(String reason, boolean locked) {
        String httpBase = getHttpBase();
        if (httpBase == null || client == null) return;
        try {
            JSONObject body = new JSONObject();
            body.put("reason", reason);
            body.put("app", lastReportedApp);
            body.put("locked", locked);
            MediaType JSON_TYPE = MediaType.get("application/json; charset=utf-8");
            RequestBody reqBody = RequestBody.create(body.toString(), JSON_TYPE);
            Request req = new Request.Builder()
                    .url(httpBase + "/api/phone-screen/skip")
                    .post(reqBody)
                    .build();
            try (Response resp = client.newCall(req).execute()) {
                Log.d(TAG, "📱 phone screen skipped " + reason + " → " + resp.code());
            }
        } catch (Exception e) {
            Log.d(TAG, "📱 phone screen skip report failed: " + e.getClass().getSimpleName() + ":" + e.getMessage());
        }
    }

    // ══════════════════════════════════════════════════════════
    //  通知渠道
    // ══════════════════════════════════════════════════════════

    private void createNotificationChannels() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return;
        NotificationManager nm = getSystemService(NotificationManager.class);
        if (nm == null) return;

        NotificationChannel c1 = new NotificationChannel(CH_KEEPALIVE, "Aion Oloth 保活",
                NotificationManager.IMPORTANCE_LOW);
        c1.setShowBadge(false);
        nm.createNotificationChannel(c1);

        NotificationChannel c2 = new NotificationChannel(CH_MESSAGE, "Aion Oloth 消息",
                NotificationManager.IMPORTANCE_DEFAULT);
        nm.createNotificationChannel(c2);

        NotificationChannel c3 = new NotificationChannel(CH_ALARM, "闹铃与监控",
                NotificationManager.IMPORTANCE_HIGH);
        c3.enableVibration(true);
        c3.setLockscreenVisibility(Notification.VISIBILITY_PUBLIC);
        nm.createNotificationChannel(c3);
    }

    private Notification buildKeepAlive(String text) {
        Intent i = new Intent(this, LauncherActivity.class);
        i.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);
        PendingIntent pi = PendingIntent.getActivity(this, 0, i,
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        return new NotificationCompat.Builder(this, CH_KEEPALIVE)
                .setSmallIcon(R.mipmap.ic_launcher)
                .setContentTitle("Aion Oloth")
                .setContentText(text)
                .setContentIntent(pi)
                .setOngoing(true)
                .setPriority(NotificationCompat.PRIORITY_LOW)
                .build();
    }

    private void updateKeepAlive(String text) {
        NotificationManager nm = getSystemService(NotificationManager.class);
        if (nm != null) nm.notify(NOTIF_FOREGROUND, buildKeepAlive(text));
    }

    // ══════════════════════════════════════════════════════════
    //  活动上报线程 — UsageStatsManager 检测前台应用
    // ══════════════════════════════════════════════════════════

    private synchronized void startActivityThread() {
        if (activityThread != null && activityThread.isAlive()) return;

        // 注册屏幕开关广播
        registerScreenReceiver();

        activityThread = new Thread(() -> {
            Log.i(TAG, "📱 Activity thread started");
            // 等待 20 秒让服务稳定
            try { Thread.sleep(20000); } catch (InterruptedException e) { return; }

            while (shouldRun) {
                try {
                    if (hasUsageStatsPermission()) {
                        reportForegroundApp();
                    } else {
                        Log.d(TAG, "📱 Usage access permission not granted");
                    }
                } catch (Exception e) {
                    Log.e(TAG, "📱 activity error: " + e.getMessage());
                }

                // 每轮检测无障碍服务，被系统关闭时自动恢复
                checkAndRecoverAccessibility();

                try { Thread.sleep(ACTIVITY_INTERVAL); }
                catch (InterruptedException e) { break; }
            }
            Log.i(TAG, "📱 Activity thread exiting");
        }, "AionActivity");
        activityThread.setDaemon(false);
        activityThread.start();
    }

    private boolean hasUsageStatsPermission() {
        try {
            UsageStatsManager usm = (UsageStatsManager) getSystemService(Context.USAGE_STATS_SERVICE);
            if (usm == null) return false;
            long now = System.currentTimeMillis();
            java.util.List<UsageStats> stats = usm.queryUsageStats(
                    UsageStatsManager.INTERVAL_DAILY, now - 60_000, now);
            return stats != null && !stats.isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    private void reportForegroundApp() {
        UsageStatsManager usm = (UsageStatsManager) getSystemService(Context.USAGE_STATS_SERVICE);
        if (usm == null) return;

        long now = System.currentTimeMillis();

        // 方案一：UsageEvents（更可靠，能在后台获取真实的前台切换事件）
        String pkgName = null;
        try {
            UsageEvents events = usm.queryEvents(now - 120_000, now);
            UsageEvents.Event event = new UsageEvents.Event();
            while (events.hasNextEvent()) {
                events.getNextEvent(event);
                // ACTIVITY_RESUMED (=1 on older / =2) 表示 Activity 进入前台
                if (event.getEventType() == UsageEvents.Event.ACTIVITY_RESUMED
                        || event.getEventType() == 1) {
                    pkgName = event.getPackageName();
                }
            }
        } catch (Exception e) {
            Log.d(TAG, "📱 UsageEvents failed, fallback to queryUsageStats: " + e.getMessage());
        }

        // 方案二：如果 UsageEvents 没结果，fallback 到 queryUsageStats
        if (pkgName == null) {
            java.util.List<UsageStats> stats = usm.queryUsageStats(
                    UsageStatsManager.INTERVAL_DAILY, now - 120_000, now);
            if (stats != null && !stats.isEmpty()) {
                UsageStats recent = null;
                for (UsageStats s : stats) {
                    if (recent == null || s.getLastTimeUsed() > recent.getLastTimeUsed()) {
                        recent = s;
                    }
                }
                if (recent != null) pkgName = recent.getPackageName();
            }
        }

        if (pkgName == null) return;

        // 仅过滤自身
        if (pkgName.equals(getPackageName())) {
            return;
        }

        // 每次轮询都上报（服务端摘要层负责合并去重）
        lastReportedApp = pkgName;
        lastReportedTime = now;

        // 直接发送包名，服务端做名称翻译（避免 vivo ROM 中文编码乱码）
        postActivityToServer(pkgName);
    }

    private void postActivityToServer(String pkgName) {
        if (serverUrl == null) return;

        String httpBase = serverUrl
                .replace("ws://", "http://")
                .replace("wss://", "https://")
                .replace("/ws", "");

        try {
            JSONObject body = new JSONObject();
            body.put("device", "phone");
            body.put("app", pkgName);
            body.put("title", pkgName);
            body.put("timestamp", System.currentTimeMillis() / 1000.0);

            MediaType JSON_TYPE = MediaType.get("application/json; charset=utf-8");
            RequestBody reqBody = RequestBody.create(body.toString(), JSON_TYPE);
            Request req = new Request.Builder()
                    .url(httpBase + "/api/activity/report")
                    .post(reqBody)
                    .build();

            try (Response resp = client.newCall(req).execute()) {
                Log.i(TAG, "📱 reported activity: " + pkgName + " → " + resp.code());
            }
        } catch (Exception e) {
            Log.e(TAG, "📱 activity report failed: " + e.getMessage());
        }
    }

    // ══════════════════════════════════════════════════════════
    //  无障碍服务自动恢复 — 被 ROM 安全策略关闭后自动重新开启
    //  需要 WRITE_SECURE_SETTINGS 权限（通过 ADB 一次性授予）：
    //  adb shell pm grant com.aion.chat android.permission.WRITE_SECURE_SETTINGS
    // ══════════════════════════════════════════════════════════

    private void checkAndRecoverAccessibility() {
        // 检查无障碍服务实例是否存活
        if (AionAccessibilityService.isReady()) return;

        // 只有用户曾主动开启过无障碍服务才自动恢复，未开过的不强制
        boolean userOptedIn = getSharedPreferences("aion_prefs", MODE_PRIVATE)
                .getBoolean("accessibility_user_opted_in", false);
        if (!userOptedIn) return;

        // 冷却期内不重复操作
        long now = System.currentTimeMillis();
        if (now - lastAccessibilityRecoverAt < ACCESSIBILITY_RECOVER_COOLDOWN) return;
        lastAccessibilityRecoverAt = now;

        // 检查是否有 WRITE_SECURE_SETTINGS 权限
        boolean hasPermission = (checkCallingOrSelfPermission(
                "android.permission.WRITE_SECURE_SETTINGS") == PackageManager.PERMISSION_GRANTED);
        if (!hasPermission) {
            Log.d(TAG, "♻️ No WRITE_SECURE_SETTINGS, cannot auto-recover accessibility");
            return;
        }

        try {
            String targetComponent = new android.content.ComponentName(
                    this, AionAccessibilityService.class).flattenToString();

            // 读取当前已启用的无障碍服务列表
            String current = Settings.Secure.getString(
                    getContentResolver(), Settings.Secure.ENABLED_ACCESSIBILITY_SERVICES);

            // 如果列表中已经没有我们的服务，重新写入
            if (current == null || !current.contains(targetComponent)) {
                String newValue = (current == null || current.isEmpty())
                        ? targetComponent
                        : current + ":" + targetComponent;
                Settings.Secure.putString(getContentResolver(),
                        Settings.Secure.ENABLED_ACCESSIBILITY_SERVICES, newValue);
                Settings.Secure.putString(getContentResolver(),
                        "accessibility_enabled", "1");
                Log.i(TAG, "♻️ Accessibility service re-enabled by WRITE_SECURE_SETTINGS");
            } else {
                // 设置里有但实例没启动，尝试先移除再添加来触发系统重新绑定
                String without = current.replace(targetComponent, "")
                        .replace("::", ":").replaceAll("^:|:$", "");
                Settings.Secure.putString(getContentResolver(),
                        Settings.Secure.ENABLED_ACCESSIBILITY_SERVICES, without);
                try { Thread.sleep(500); } catch (InterruptedException ignored) {}
                String restored = without.isEmpty()
                        ? targetComponent
                        : without + ":" + targetComponent;
                Settings.Secure.putString(getContentResolver(),
                        Settings.Secure.ENABLED_ACCESSIBILITY_SERVICES, restored);
                Log.i(TAG, "♻️ Accessibility service toggled to force rebind");
            }
        } catch (SecurityException e) {
            Log.w(TAG, "♻️ WRITE_SECURE_SETTINGS permission revoked: " + e.getMessage());
        } catch (Exception e) {
            Log.e(TAG, "♻️ accessibility recover failed: " + e.getMessage());
        }
    }

    // ══════════════════════════════════════════════════════════
    //  屏幕开关监听 — 锁屏/亮屏时立即上报
    // ══════════════════════════════════════════════════════════

    private void registerScreenReceiver() {
        if (screenReceiver != null) return;
        screenReceiver = new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, Intent intent) {
                if (intent == null || intent.getAction() == null) return;
                switch (intent.getAction()) {
                    case Intent.ACTION_SCREEN_OFF:
                        Log.i(TAG, "📱 Screen OFF");
                        screenOn = false;
                        lastReportedApp = "__screen_off__";
                        // 在后台线程发送，避免阻塞广播
                        new Thread(() -> {
                            postActivityToServer("screen_off");
                            postPhoneScreenSkip("screen_off", true);
                        }, "ScreenOff").start();
                        break;
                    case Intent.ACTION_SCREEN_ON:
                        Log.i(TAG, "📱 Screen ON");
                        screenOn = true;
                        lastReportedApp = "__screen_on__";
                        new Thread(() -> {
                            postActivityToServer("screen_on");
                        }, "ScreenOn").start();
                        break;
                }
            }
        };
        IntentFilter filter = new IntentFilter();
        filter.addAction(Intent.ACTION_SCREEN_OFF);
        filter.addAction(Intent.ACTION_SCREEN_ON);
        registerReceiver(screenReceiver, filter);
        Log.i(TAG, "📱 Screen receiver registered");
    }

    private void unregisterScreenReceiver() {
        if (screenReceiver != null) {
            try { unregisterReceiver(screenReceiver); } catch (Exception ignored) {}
            screenReceiver = null;
        }
    }

    // ══════════════════════════════════════════════════════════
    //  步数计数 — TYPE_STEP_COUNTER 传感器 + 重启补偿 + 5:00 重置
    // ══════════════════════════════════════════════════════════

    /**
     * 获取当前"逻辑日期"字符串（以凌晨 5:00 为分界）。
     * 例如：若当前时间是 2026-05-15 03:00，逻辑上仍属于 "2026-05-14"。
     */
    private String getLogicalDate() {
        Calendar cal = Calendar.getInstance();
        if (cal.get(Calendar.HOUR_OF_DAY) < STEP_RESET_HOUR) {
            cal.add(Calendar.DATE, -1);
        }
        return new SimpleDateFormat("yyyy-MM-dd", Locale.US).format(cal.getTime());
    }

    private void initStepCounter() {
        if (sensorManager == null) {
            sensorManager = (SensorManager) getSystemService(Context.SENSOR_SERVICE);
        }
        if (sensorManager == null) {
            Log.w(TAG, "\uD83D\uDC63 SensorManager not available");
            return;
        }
        if (stepSensor != null) return;  // 已经注册过了
        stepSensor = sensorManager.getDefaultSensor(Sensor.TYPE_STEP_COUNTER);
        if (stepSensor == null) {
            Log.w(TAG, "\uD83D\uDC63 No step counter sensor on this device");
            return;
        }
        // 重装 APK 后 SharedPreferences 丢失，尝试从服务端恢复步数基线
        SharedPreferences prefs = getSharedPreferences("aion_prefs", MODE_PRIVATE);
        if (prefs.getFloat(PREF_STEP_DAY_START, -1) < 0) {
            stepRestorePending = true;
            restoreStepStateFromServer();
        }
        // 传感器回调必须在有 Looper 的线程上注册，用主线程 Handler
        sensorManager.registerListener(stepListener, stepSensor,
                SensorManager.SENSOR_DELAY_NORMAL, mainHandler);
        Log.i(TAG, "\uD83D\uDC63 Step counter sensor registered (mainHandler)");
    }

    /**
     * 从服务端恢复步数状态（重装 APK 后 SharedPreferences 丢失时调用）
     */
    private void restoreStepStateFromServer() {
        if (serverUrl == null) {
            stepRestorePending = false;
            return;
        }
        new Thread(() -> {
            try {
                String httpBase = serverUrl.replace("ws://", "http://")
                        .replace("wss://", "https://")
                        .replace("/ws", "");
                String apiUrl = httpBase + "/api/location/step-state";
                Request req = new Request.Builder().url(apiUrl).get().build();
                try (Response resp = client.newCall(req).execute()) {
                    String body = resp.body() != null ? resp.body().string() : "";
                    JSONObject json = new JSONObject(body);
                    int steps = json.optInt("steps", -1);
                    String date = json.optString("logical_date", "");
                    if (steps > 0 && date.equals(getLogicalDate())) {
                        serverStepRestore = steps;
                        Log.i(TAG, "\uD83D\uDC63 Restored step state from server: " + steps + " steps for " + date);
                    } else {
                        Log.i(TAG, "\uD83D\uDC63 No matching step state on server (steps=" + steps + " date=" + date + " today=" + getLogicalDate() + ")");
                    }
                }
            } catch (Exception e) {
                Log.w(TAG, "\uD83D\uDC63 Failed to restore step state: " + e.getMessage());
            } finally {
                stepRestorePending = false;
            }
        }).start();
    }

    private final SensorEventListener stepListener = new SensorEventListener() {
        @Override
        public void onSensorChanged(SensorEvent event) {
            if (event.sensor.getType() != Sensor.TYPE_STEP_COUNTER) return;
            float currentCounter = event.values[0];
            latestStepCounter = currentCounter;

            SharedPreferences prefs = getSharedPreferences("aion_prefs", MODE_PRIVATE);
            String savedDate = prefs.getString(PREF_STEP_RESET_DATE, "");
            String logicalDate = getLogicalDate();

            float dayStart = prefs.getFloat(PREF_STEP_DAY_START, -1);
            float lastKnown = prefs.getFloat(PREF_STEP_LAST_KNOWN, -1);
            float rebootOffset = prefs.getFloat(PREF_STEP_REBOOT_OFFSET, 0);

            // 首次启动或跨逻辑日 → 重置
            if (!logicalDate.equals(savedDate) || dayStart < 0) {
                // 等待服务端恢复完成（重装 APK 场景）
                if (dayStart < 0 && stepRestorePending) {
                    Log.d(TAG, "\uD83D\uDC63 Waiting for server step restore...");
                    return;
                }
                // 重装 APK 后从服务端恢复的步数作为 rebootOffset
                float restoreOffset = 0;
                if (dayStart < 0 && serverStepRestore > 0) {
                    restoreOffset = serverStepRestore;
                    serverStepRestore = -1;
                    Log.i(TAG, "\uD83D\uDC63 Using server-restored steps as offset: " + (int) restoreOffset);
                }
                Log.i(TAG, "\uD83D\uDC63 Step reset for logical day " + logicalDate
                        + " (was " + savedDate + ") restoreOffset=" + (int) restoreOffset);
                prefs.edit()
                        .putFloat(PREF_STEP_DAY_START, currentCounter)
                        .putFloat(PREF_STEP_REBOOT_OFFSET, restoreOffset)
                        .putFloat(PREF_STEP_LAST_KNOWN, currentCounter)
                        .putString(PREF_STEP_RESET_DATE, logicalDate)
                        .apply();
                return;
            }

            // 重启检测：传感器值小于上次记录值 → 手机重启了
            if (lastKnown >= 0 && currentCounter < lastKnown) {
                float rescued = lastKnown - dayStart;
                rebootOffset += rescued;
                dayStart = 0;  // TYPE_STEP_COUNTER 重启后从 0 开始
                Log.i(TAG, "\uD83D\uDC63 Reboot detected! rescued=" + (int) rescued
                        + " newOffset=" + (int) rebootOffset);
                prefs.edit()
                        .putFloat(PREF_STEP_DAY_START, dayStart)
                        .putFloat(PREF_STEP_REBOOT_OFFSET, rebootOffset)
                        .putFloat(PREF_STEP_LAST_KNOWN, currentCounter)
                        .apply();
                return;
            }

            // 正常更新 lastKnown
            prefs.edit().putFloat(PREF_STEP_LAST_KNOWN, currentCounter).apply();
        }

        @Override
        public void onAccuracyChanged(Sensor sensor, int accuracy) {}
    };

    /**
     * 获取今日步数。返回 -1 表示传感器不可用。
     */
    private int getTodaySteps() {
        if (latestStepCounter < 0) return -1;

        SharedPreferences prefs = getSharedPreferences("aion_prefs", MODE_PRIVATE);
        String savedDate = prefs.getString(PREF_STEP_RESET_DATE, "");
        String logicalDate = getLogicalDate();

        // 跨日但传感器回调还没触发重置，先算旧日步数返回 0 也行
        // 但更安全的做法是在这里也做重置
        if (!logicalDate.equals(savedDate)) {
            prefs.edit()
                    .putFloat(PREF_STEP_DAY_START, latestStepCounter)
                    .putFloat(PREF_STEP_REBOOT_OFFSET, 0)
                    .putFloat(PREF_STEP_LAST_KNOWN, latestStepCounter)
                    .putString(PREF_STEP_RESET_DATE, logicalDate)
                    .apply();
            return 0;
        }

        float dayStart = prefs.getFloat(PREF_STEP_DAY_START, latestStepCounter);
        float rebootOffset = prefs.getFloat(PREF_STEP_REBOOT_OFFSET, 0);
        int steps = (int) ((latestStepCounter - dayStart) + rebootOffset);
        return Math.max(steps, 0);
    }
}
