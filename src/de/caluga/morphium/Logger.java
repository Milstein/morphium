package de.caluga.morphium;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by stephan on 25.04.15.
 */
public class Logger {
    private int level = 5;
    private String prfx;
    private DateFormat df = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSS");
    private String file;
    private PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out));
    private boolean synced = false;
    private boolean closeIt = true;

    public Logger(String name) {
        prfx = name;

        String v = getSetting("log.level");
        if (getSetting("log.level." + name) != null) {
            v = getSetting("log.level." + name);
        }

        if (v != null) level = Integer.parseInt(v);

        v = getSetting("log.file");
        if (getSetting("log.file." + name) != null) v = getSetting("log.file." + name);
        if (v != null) {
            file = v;
            if (v.equals("-") || v.equals("STDOUT")) {
                out = new PrintWriter(new OutputStreamWriter(System.out));
                closeIt = false;
            } else if (v.equals("STDERR")) {
                out = new PrintWriter(new OutputStreamWriter(System.err));
                closeIt = false;
            } else {
                try {
                    out = new PrintWriter(new BufferedWriter(new FileWriter(v, true)));
                    closeIt = true;
                } catch (IOException e) {
                    error(null, e);
                }
            }
        }

        v = getSetting("log.synced");
        if (getSetting("log.synced." + name) != null) v = getSetting("log.synced." + name);
        if (v != null) {
            synced = v.equals("true");
        }

//        info("Logger " + name + " instanciated: Level: " + level + " Synced: " + synced + " file: " + file);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        out.flush();
        if (closeIt)
            out.close();

    }

    public String getFile() {
        return file;
    }

    public void setFile(String v) {
        this.file = v;
        file = v;
        out.flush();
        out.close();
        if (file == null) {
            out = new PrintWriter(new OutputStreamWriter(System.out));
        } else {
            try {
                out = new PrintWriter(new BufferedWriter(new FileWriter(v, true)));
            } catch (IOException e) {
                error(null, e);
            }
        }
    }

    private String getSetting(String s) {
        String v = null;
        s = "morphium." + s;
        if (System.getenv(s.replaceAll("\\.", "_")) != null) {
            v = System.getenv(s.replaceAll("\\.", "_"));
        }
        if (System.getProperty(s) != null) {
            v = System.getProperty(s);
        }
        if (v == null) {
            //no setting yet, looking for prefixes
            int lng = 0;
            for (Map.Entry<Object, Object> p : System.getProperties().entrySet()) {
                if (s.startsWith(p.getKey().toString())) {
                    //prefix found
                    if (p.getKey().toString().length() > lng) {
                        //keeping the longest prefix
                        //if s== log.level.de.caluga.morphium.ObjetMapperImpl
                        // property: morphium.log.level.de.caluga.morphium=5
                        // property: morphium.log.level.de.caluga=0
                        // => keep only the longer one (5)
                        lng = p.getKey().toString().length();
                        v = p.getValue().toString();
                    }
                }
            }
        }

        return v;
    }

    public void setLevel(int lv) {
        level = lv;
    }

    public void setLevel(Object o) {
        //ignore
    }

    public boolean isEnabledFor(Object o) {
        return true;
    }

    public boolean isDebugEnabled() {
        return level >= 5;
    }

    public boolean isInfoEnabled() {
        return level >= 4;
    }

    public boolean isWarnEnabled() {
        return level >= 3;
    }

    public boolean isErrorEnabled() {
        return level >= 2;
    }

    public boolean isFatalEnabled() {
        return level >= 1;
    }

    public Logger(Class cls) {
        this(cls.getName());
    }


    public void info(Object msg) {
        info(msg.toString(), null);
    }

    public void info(String msg) {
        info(msg, null);
    }

    public void info(Throwable t) {
        info(null, t);
    }

    public void info(String msg, Throwable t) {
        doLog(4, msg, t);

    }

    public void debug(Object msg) {
        debug(msg.toString());
    }

    public void debug(String msg) {
        debug(msg, null);
    }

    public void debug(Throwable t) {
        debug(null, t);
    }

    public void debug(String msg, Throwable t) {
        doLog(5, msg, t);

    }

    public void warn(Object msg) {
        warn(msg.toString(), null);
    }

    public void warn(String msg) {
        warn(msg, null);
    }

    public void warn(Throwable t) {
        warn(null, t);
    }

    public void warn(String msg, Throwable t) {
        doLog(3, msg, t);

    }

    public void error(Object msg) {
        error(msg.toString(), null);
    }

    public void error(String msg) {
        error(msg, null);
    }

    public void error(Throwable t) {
        error(null, t);
    }

    public void error(String msg, Throwable t) {
        doLog(2, msg, t);

    }

    public void fatal(Object msg) {
        fatal(msg.toString(), null);
    }

    public void fatal(String msg) {
        fatal(msg, null);
    }

    public void fatal(Throwable t) {
        fatal(null, t);
    }

    public void fatal(String msg, Throwable t) {
        doLog(1, msg, t);
    }

    private void doLog(int lv, String msg, Throwable t) {
        if (level >= lv) {
            out.print(df.format(new Date()));
            out.print(":");
            switch (lv) {
                case 5:
                    out.print(" DEBUG ");
                    break;
                case 4:
                    out.print(" INFO ");
                    break;
                case 3:
                    out.print(" WARN ");
                    break;
                case 2:
                    out.print(" ERROR ");
                    break;
                case 1:
                    out.print(" FATAL ");
                    break;
                default:
                    return;
            }
            out.print("[");
            StackTraceElement[] st = new Exception().getStackTrace();
            int idx = 0;
            while (st[idx].getClassName().equals(this.getClass().getName()) && idx <= st.length) idx++;
            out.print(st[idx].getClassName());
            out.print(".");
            out.print(st[idx].getMethodName());
            out.print("():");
            out.print(st[idx].getLineNumber());
            out.print(" - ");
            if (msg != null) {
                out.print(msg);
            }
            out.println();
            if (t != null) {
                t.printStackTrace();
                ;
            }
            if (synced)
                out.flush();

        }
    }

    public boolean isSynced() {
        return synced;
    }

    public void setSynced(boolean synced) {
        this.synced = synced;
    }
}
