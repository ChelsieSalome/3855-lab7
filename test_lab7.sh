#!/bin/bash

echo "=========================================="
echo "  ACIT 3855 Lab 7 - Complete System Test"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test 1: Service Health
echo -e "${BLUE}1️⃣  Checking Service Health${NC}"
echo "-----------------------------------"
docker-compose ps
echo ""

# Test 2: Send 5 Performance Events
echo -e "${BLUE}2️⃣  Sending 5 Performance Events${NC}"
echo "-----------------------------------"
for i in {1..5}; do
  curl -s -X POST http://localhost:8080/monitoring/performance \
    -H "Content-Type: application/json" \
    -d "{
      \"server_id\": \"server-00$i\",
      \"reporting_timestamp\": \"2026-03-04T02:$(printf '%02d' $i):00Z\",
      \"metrics\": [
        {
          \"cpu\": $((60 + i * 5)).5,
          \"memory\": $((50 + i * 3)).2,
          \"disk_io\": $((100 + i * 10)).8
        }
      ]
    }" > /dev/null
  echo -e "${GREEN}✓${NC} Sent performance event $i"
done
echo ""

# Test 3: Send 3 Error Events
echo -e "${BLUE}3️⃣  Sending 3 Error Events${NC}"
echo "-----------------------------------"
ERROR_CODES=("500" "503" "404")
SEVERITIES=(3 4 2)
for i in {0..2}; do
  curl -s -X POST http://localhost:8080/monitoring/errors \
    -H "Content-Type: application/json" \
    -d "{
      \"server_id\": \"server-err-00$i\",
      \"reporting_timestamp\": \"2026-03-04T02:$(printf '%02d' $((i + 10))):00Z\",
      \"errors\": [
        {
          \"error_code\": \"${ERROR_CODES[$i]}\",
          \"severity_level\": ${SEVERITIES[$i]},
          \"avg_response_time\": $((1000 + i * 500)).5,
          \"error_message\": \"Error type ${ERROR_CODES[$i]}\"
        }
      ]
    }" > /dev/null
  echo -e "${GREEN}✓${NC} Sent error event $((i + 1)) (code: ${ERROR_CODES[$i]})"
done
echo ""

# Wait for Kafka processing
echo -e "${YELLOW} Waiting 5 seconds for Kafka processing...${NC}"
sleep 5
echo ""

# Test 4: Check Statistics
echo -e "${BLUE}4️⃣  Event Statistics${NC}"
echo "-----------------------------------"
curl -s http://localhost:5005/analyzer/stats | python3 -m json.tool
echo ""

# Test 5: Retrieve Performance Events
echo -e "${BLUE}5️⃣  Sample Performance Events${NC}"
echo "-----------------------------------"
echo "First event (index 0):"
curl -s "http://localhost:5005/analyzer/performance?index=0" | python3 -m json.tool
echo ""
echo "Last event (index 4):"
curl -s "http://localhost:5005/analyzer/performance?index=4" | python3 -m json.tool
echo ""

# Test 6: Retrieve Error Events
echo -e "${BLUE}6️⃣  Sample Error Events${NC}"
echo "-----------------------------------"
echo "First error (index 0):"
curl -s "http://localhost:5005/analyzer/error?index=0" | python3 -m json.tool
echo ""
echo "Last error (index 2):"
curl -s "http://localhost:5005/analyzer/error?index=2" | python3 -m json.tool
echo ""

# Test 7: Invalid Index Tests
echo -e "${BLUE}7️⃣  Error Handling Tests${NC}"
echo "-----------------------------------"
echo "Testing invalid performance index (999):"
curl -s "http://localhost:5005/analyzer/performance?index=999" | python3 -m json.tool
echo ""
echo "Testing invalid error index (999):"
curl -s "http://localhost:5005/analyzer/error?index=999" | python3 -m json.tool
echo ""

# Test 8: Kafka Topic Verification
echo -e "${BLUE}8️⃣  Kafka Topic Contents (Last 10 messages)${NC}"
echo "-----------------------------------"
docker exec $(docker ps -q -f name=kafka) kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 3000 2>/dev/null | python3 -m json.tool
echo ""

# Test 9: Service Logs
echo -e "${BLUE}9️⃣  Recent Service Logs${NC}"
echo "-----------------------------------"
echo "Receiver logs:"
docker-compose logs --tail=5 receiver-service
echo ""
echo "Analyzer logs:"
docker-compose logs --tail=5 analyzer-service
echo ""

echo "=========================================="
echo -e "${GREEN} Complete System Test Finished!${NC}"
echo "=========================================="
echo ""
echo "Summary:"
echo "- Performance events sent: 5"
echo "- Error events sent: 3"
echo "- Total events in system: 8 (including previous test events)"
echo ""
echo "Next steps:"
echo "1. Review the statistics endpoint output"
echo "2. Verify event retrieval by index works"
echo "3. Check that trace_id is unique for each event"
echo "4. Confirm error handling for invalid indexes"
