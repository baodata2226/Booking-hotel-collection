from seleniumbase import SB
import random
import pandas as pd
from tabulate import tabulate
import re
from unidecode import unidecode


# 1. Bảng Fre_Question: id, hotel_id, question, answer
# 2. Bảng Review: id, comment, score, createdAt, username
# 3. Bảng Hotel: id, name, location, description, score
# 4. Bảng Hotel_Image: id, hotel_id, img_url
# 5. Bảng Facility: id, hotel_id, facilities ({type: {name, is_charged}}), most_popular_facilities (icon, name)
# 6. Bảng Room: id, hotel_id, type, price, service ({icon:name}), discount
# 7. Bảng Policy: hotel_id, info ({check-in, check-out, cancel, children, age, pet, paying_method})
# 8. Bảng Service: id, name, icon


# Danh sách các tỉnh thành Việt Nam
vn_provinces = [
    "An Giang", "Bà Rịa - Vũng Tàu", "Bạc Liêu", "Bắc Giang", "Bắc Kạn",
    "Bắc Ninh", "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước",
    "Bình Thuận", "Cà Mau", "Cao Bằng", "Cần Thơ", "Đà Nẵng",
    "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp",
    "Gia Lai", "Hà Giang", "Hà Nam", "Hà Nội", "Hà Tĩnh",
    "Hải Dương", "Hải Phòng", "Hậu Giang", "Hòa Bình", "Hưng Yên",
    "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu", "Lâm Đồng",
    "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", "Nghệ An",
    "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Phú Yên", "Quảng Bình",
    "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng",
    "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên", "Thanh Hóa",
    "Thừa Thiên Huế", "Tiền Giang", "TP Hồ Chí Minh", "Trà Vinh",  # -------------
    "Tuyên Quang", "Vĩnh Long", "Vĩnh Phúc", "Yên Bái"
]


def extract_hotel(_hotel_elements, _hotels_data):
    for hotel in _hotel_elements:
        # Extract hotel details within each hotel container_hotel using sub-selectors
        hotel_url = re.match(r"^[^?]+", hotel.find_element('css selector', 'a').get_attribute('href')).group(0)

        _hotels_data.append({
            'Url': hotel_url
        })


# Function to randomize delays
def random_delay():
    return random.uniform(5, 6)


for province in vn_provinces[3:4]:
    # Tạo URL cho mỗi tỉnh
    base_url = "https://www.booking.com/searchresults.vi.html?ss="
    province_url = base_url + province.replace(' ', '+')
    province = unidecode(province)
    print(province_url)
    # proxy = get_random_proxy()

    with SB(uc=True) as sb:
        sb.sleep(20)
        sb.open(province_url)
        sb.maximize_window()
        sb.sleep(random_delay())
        x_coordinate = 200  # X position
        y_coordinate = 600  # Y position
        sb.execute_script(f"document.elementFromPoint({x_coordinate}, {y_coordinate}).click();")
        sb.sleep(random_delay())

        try:
            close_pop_up = sb.find_element('css selector', 'button[aria-label="Bỏ qua phần đăng nhập."]')
            close_pop_up.click()
        except Exception as e:
            print(e)
        sb.sleep(random_delay())

        last_height = sb.execute_script("return document.body.scrollHeight")
        previous_count = 0  # Track the count of hotels loaded

        while True:
            sb.scroll_to_bottom()  # Scroll to the bottom of the page
            sb.sleep(random_delay())  # Wait for content to load

            # Check for "Show More" button and click it if available
            try:
                show_more_button = sb.find_element('css selector', 'button[class="a83ed08757 c21c56c305 '
                                                                   'bf0537ecb5 f671049264 af7297d90d c0e0affd09"]')
                show_more_button.click()
                sb.sleep(random_delay())
            except:
                pass

            # Check if new hotel elements have loaded by comparing counts
            hotel_elements = sb.find_elements('css selector', 'div[data-testid="property-card"]')
            current_count = len(hotel_elements)

            # If the count hasn't changed, assume end of loading
            if current_count == previous_count:
                break
            else:
                previous_count = current_count  # Update previous count

            # Check for new scroll height and update if it's changed
            new_height = sb.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break

            last_height = new_height  # Update last height for the next check

        # Initialize a list for hotel details
        hotels_data = []
        hotel_elements = sb.find_elements('css selector', 'div[data-testid="property-card"]')
        extract_hotel(hotel_elements, hotels_data)

        # Create a DataFrame from the extracted data
        df_hotels = pd.DataFrame(hotels_data)
        df_hotels.drop_duplicates(inplace=True)

        # Save to CSV file
        csv_filename = f"{province}_urls.csv"
        df_hotels.to_csv(csv_filename, index=False)

        # displaying the DataFrame
        print(tabulate(df_hotels, headers='keys', tablefmt='fancy_grid'))
        print(df_hotels.shape)

        # print("Now waiting for inspecting...")
        # sb.sleep(999999)

# Challenges
# 0. Can't use different region IP for website in Vietnam
# 1. No <a> tag in usual html code: Try convert into mobile view, the <a>
#    tag might appear.
# 2. Scrolling infinity:
#  + In computer view: Using Javascript code to measure browser's height
#                      to detect dynamically content when scrolling, we also
#                      use this to break the while loop when reaching the end
#                      of the website.
#  + In mobile view: It is hard to simulate scrolling in mobile view.
#                    A good way to scrolling is that we will scroll to
#                    the last element that we are scraping. But first,
#                    try to look for the scrollable container that contains
#                    the content we want to scrape.
#  * Note that: Some HTML code remove the above content when we scroll away.
#               Hence, try to scroll so that we can catch all the content.
#               (Scroll a specified vertical pixel, scroll to the last content)
# 3. Using tabulate for showing pretty pandas table.
# 4. Checking if one can trim the length of the item's URL for easier management.
#    Since the URL sometimes contain SearchID... that is not neccessary.


# Ensemble: Tổng hợp lại đáp án của nhiều mô hình (giữ lại bounding box đúng nhất
# Co-training: quản lý việc học của nhiều mô hình (If else phân loại ảnh ngày/đêm)
# Khắc phục imbalance giữa các class: Chia để trị (Nhóm 5 <--> Nhóm 4, 6, 7) <---> Train nhị phân nhiều lần
# Những bbox lớn có số lượng ít bỏ luôn (outliers) vì nó phá weights (IOU)
# Mô hình đọc được mà không có ground truth thì nó là background (có object nhưng BTC không label)
