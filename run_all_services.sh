echo "Starting all services..."

cd build

./order_service &
./payment_service &
./inventory_service &
./monitor_service &
./notification_service &

echo "All services started successfully."
