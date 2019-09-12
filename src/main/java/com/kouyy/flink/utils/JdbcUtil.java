package com.kouyy.flink.utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * C3P0连接池的工具类
 */
public class JdbcUtil {

        private static ComboPooledDataSource dataSource = new ComboPooledDataSource();

        // 添加线程局部变量
        private static ThreadLocal<Connection> local = new ThreadLocal<Connection>();

        public static Connection getConnection() throws SQLException {
            // 从本地线程变量ThreadLocal中获取连接
            Connection conn = local.get();
            // 是第一次获取
            if (conn == null) {
                // 从连接池获得一个连接
                conn = dataSource.getConnection();
                // 将连接添加到本地线程变量中共享
                local.set(conn);
            }
            return conn;
        }

    public static void closeConnection() throws SQLException {
        // 从本地线程变量ThreadLocal中获取连接
        Connection conn = local.get();
        // 是第一次获取
        if (conn != null) {
            conn.close();
        }
    }


        public static DataSource getDataSource() {
            return dataSource;
        }

    /**
     * 查询
     */
    public static ResultSet querySQL(String sql){
        ResultSet resultSet=null;
        try{
            DataSource dataSource = JdbcUtil.getDataSource();
            Connection connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
        }catch(Exception e){
                System.out.println("查询数据库失败！");
            }
        return resultSet;
    }

    public static Boolean insertSQL(String sql) throws SQLException {
        Boolean result=false;
        Connection connection =null;
        try{
            DataSource dataSource = JdbcUtil.getDataSource();
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            result = preparedStatement.execute(sql);
        }catch(Exception e){
            rollbackAndClose(connection);
            System.out.println("插入数据库失败！");
        }finally {
            commitAndClose(connection);
            connection.close();
        }
        return result;
    }



        /**
         * 提交事务&关闭连接＆从本地线程变量移除连接
         *
         * @param conn
         */
        public static void commitAndClose(Connection conn) {
            try {
                // 提交事务
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                // 关闭连接
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    // 从本地线程变量移除连接
                    local.remove();
                }

            }

        }

        /**
         * 回滚事务&关闭连接＆从本地线程变量移除连接
         *
         * @param conn
         */
        public static void rollbackAndClose(Connection conn) {
            try {
                // 回滚事务
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                // 关闭连接
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    // 从本地线程变量移除连接
                    local.remove();
                }

            }
        }
}
