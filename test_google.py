from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    url = 'https://search.yahoo.com/search?p=site:facebook.com+"cafes"+"Pune"+-login+-signin+-signup+-auth&b=1'
    page.goto(url)
    
    print("Page URL:", page.url)
    print("Page Title:", page.title())
    
    items1 = page.locator('.algo-title')
    items2 = page.locator('div.compTitle a')
    
    print(f"Found {items1.count()} items using .algo-title")
    print(f"Found {items2.count()} items using div.compTitle a")
    
    if items2.count() > 0:
        first = items2.first
        print("First item href:", first.get_attribute('href'))
        
    browser.close()
