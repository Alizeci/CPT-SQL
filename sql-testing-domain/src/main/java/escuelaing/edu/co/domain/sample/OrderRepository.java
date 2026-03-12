package escuelaing.edu.co.domain.sample;

import java.sql.*;
import java.util.*;

import escuelaing.edu.co.processor.annotation.Req;
import escuelaing.edu.co.processor.annotation.SqlQuery;

@Req(
    maxResponseTimeMs = 500,
    priority = Req.Priority.MEDIUM,
    description = "Default del repositorio de órdenes"
)
public class OrderRepository {

    @SqlQuery(queryId = "getUserOrders", description = "Órdenes activas de un usuario")
    @Req(maxResponseTimeMs = 200, priority = Req.Priority.HIGH,
         description = "Crítica del checkout", allowPlanChange = false)
    public List<String> getUserOrders(Connection conn, long userId) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(
            "SELECT id FROM orders WHERE user_id = ? AND status = 'ACTIVE'");
        ps.setLong(1, userId);
        ResultSet rs = ps.executeQuery();
        List<String> result = new ArrayList<>();
        while (rs.next()) result.add(rs.getString("id"));
        return result;
    }

    @SqlQuery(queryId = "insertOrder", description = "Inserta una nueva orden")
    public void insertOrder(Connection conn, long userId, double total) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO orders (user_id, total, status) VALUES (?, ?, 'PENDING')");
        ps.setLong(1, userId);
        ps.setDouble(2, total);
        ps.executeUpdate();
    }

    @SqlQuery(queryId = "getOrderHistory", description = "Historial completo de órdenes")
    public List<String> getOrderHistory(Connection conn, long userId) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(
            "SELECT id FROM orders WHERE user_id = ? ORDER BY created_at DESC");
        ps.setLong(1, userId);
        ResultSet rs = ps.executeQuery();
        List<String> result = new ArrayList<>();
        while (rs.next()) result.add(rs.getString("id"));
        return result;
    }
}