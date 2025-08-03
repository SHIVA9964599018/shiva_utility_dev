// ✅ script.js (module-compatible with full function support)
import { createClient } from "https://cdn.jsdelivr.net/npm/@supabase/supabase-js/+esm";

const supabaseClient = createClient(
  "https://wzgchcvyzskespcfrjvi.supabase.co",
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind6Z2NoY3Z5enNrZXNwY2ZyanZpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE4NjQwNDEsImV4cCI6MjA1NzQ0MDA0MX0.UuAgu4quD9Vg80tOUSkfGJ4doOT0CUFEUeoHsiyeNZQ"
);
localStorage.removeItem("user_id"); // ✅ run IMMEDIATELY on script load

window.dishNames = [];

window.showSection = function (sectionId) {
  console.log(`Switching to Section: ${sectionId}`);

  // Hide all sections
  document.querySelectorAll("section, div[id^='utility-']").forEach((el) => {
    el.style.display = "none";
  });

  // Show main utility section if needed
  if (sectionId.startsWith("utility-")) {
    const utilitiesSection = document.getElementById("utilities");
    if (utilitiesSection) {
      utilitiesSection.style.display = "block";
    }
  }

  // Show the requested section
  const targetSection = document.getElementById(sectionId);
  if (targetSection) {
    targetSection.style.display = "block";
  } else {
    console.error(`Section '${sectionId}' not found`);
  }
};

// Show utility sub-section
window.showUtilitySubSection = function (sectionId) {
  const sections = document.querySelectorAll("#utilities > div");
  sections.forEach(sec => sec.style.display = "none");

  const target = document.getElementById(sectionId);
  console.log('the section id clicked');
  if (target) target.style.display = "block";
};


let dishNames = [];

window.loadDishNames = async function () {
  const { data, error } = await supabaseClient.from("food_items").select("dish_name");
  if (!error && data) {
    dishNames = data.map(d => d.dish_name);
  } else {
    console.error("Failed to load dish names:", error);
  }
};

window.addEventListener("DOMContentLoaded", () => {
  loadDishNames(); // make sure this is called on page load
});



window.setupAutocomplete = function (input) {
  // Wrap input in a relative container
  const wrapper = document.createElement("div");
  wrapper.style.position = "relative";
  input.parentNode.insertBefore(wrapper, input);
  wrapper.appendChild(input);

  // Create dropdown box
  const box = document.createElement("div");
  box.className = "autocomplete-box";
  box.style.position = "absolute";
  box.style.top = input.offsetHeight + "px";
  box.style.left = "0";
  box.style.right = "0";
  box.style.zIndex = "9999";
  box.style.background = "white";
  box.style.border = "1px solid #ccc";
  box.style.borderTop = "none";
  box.style.maxHeight = "150px";
  box.style.overflowY = "auto";
  box.style.display = "none";

  wrapper.appendChild(box);

  input.addEventListener("input", function () {
    const val = input.value.trim().toLowerCase();
    box.innerHTML = "";

    if (!val) {
      box.style.display = "none";
      return;
    }

    const matches = dishNames.filter(name =>
      name.toLowerCase().includes(val)
    ).slice(0, 10);

    if (matches.length === 0) {
      box.style.display = "none";
      return;
    }

    matches.forEach(match => {
      const div = document.createElement("div");
      div.textContent = match;
      div.style.padding = "6px";
      div.style.cursor = "pointer";
      div.style.borderBottom = "1px solid #eee";
      div.addEventListener("click", () => {
        input.value = match;
        box.innerHTML = "";
        box.style.display = "none";
      });
      box.appendChild(div);
    });

    box.style.display = "block";
  });

  // Hide when clicking outside
  document.addEventListener("click", function (e) {
    if (!wrapper.contains(e.target)) {
      box.style.display = "none";
    }
  });
};



// ✅ Add a dish row
window.addDishRow = function (mealType, name = "", grams = "") {
console.log(`clicked on Add for ${mealType}`);
  const container = document.getElementById(`${mealType}-container`);
  const row = document.createElement("div");
  row.className = "dish-row";

  row.innerHTML = `
    <input type="text" class="dish-name responsive-dish-name" value="${name}" placeholder="Dish Name" />
    <input type="number" class="dish-grams responsive-dish-grams" value="${grams}" placeholder="gms" />
    <button class="remove-btn" onclick="this.parentElement.remove()">Remove</button>
  `;

  container.appendChild(row);

  const input = row.querySelector(".dish-name");
  setupAutocomplete(input); // autocomplete support
};




// ✅ Fetch dish info
window.getDishInfo = async function (name) {
  const { data, error } = await supabaseClient
    .from("food_items")
    .select("*")
    .ilike("dish_name", name.trim());

  if (!error && data && data.length) return data[0];
  return null;
};


// ✅ Calculate total macros
window.calculateCalories = async function () {
  const meals = ["breakfast", "lunch", "dinner"];
  let totals = { calories: 0, protein: 0, carbs: 0, fibre: 0, fats: 0 };
  let today = new Date().toISOString().split("T")[0];
  let dishEntries = [];

  for (const meal of meals) {
    const container = document.getElementById(`${meal}-container`);
    const rows = container.querySelectorAll(".dish-row");

    for (const row of rows) {
      const name = row.querySelector(".dish-name").value;
      const grams = parseFloat(row.querySelector(".dish-grams").value);
      if (!name || isNaN(grams)) continue;

      const info = await window.getDishInfo(name);
      if (!info) continue;

      const factor = grams / 100;
      totals.calories += (info.calorie_per_100gm || 0) * factor;
      totals.protein += (info.protein_per_100gm || 0) * factor;
      totals.carbs += (info.carbs_per_100gm || 0) * factor;
      totals.fibre += (info.fibre_per_100gm || 0) * factor;
      totals.fats += (info.fats_per_100gm || 0) * factor;

      dishEntries.push({
        date: today,
        meal_type: meal,
        dish_name: name,
        quantity_grams: grams
      });
    }
  }

  // Show calculated totals
document.getElementById("calorie-result").innerHTML = `
  <div style="display: flex; flex-direction: column; gap: 3px; font-family: Arial, sans-serif; font-size: 1rem; margin-top: 10px;">
    <div style="background: #1976d2; color: white; padding: 8px 16px; border-radius: 20px; width: 160px; display: flex; justify-content: space-between;">
      <span>Calories:</span><span>${totals.calories.toFixed(0)}</span>
    </div>
    <div style="background: #1976d2; color: white; padding: 8px 16px; border-radius: 20px; width: 160px; display: flex; justify-content: space-between;">
      <span>Protein:</span><span>${totals.protein.toFixed(0)}</span>
    </div>
    <div style="background: #1976d2; color: white; padding: 8px 16px; border-radius: 20px; width: 160px; display: flex; justify-content: space-between;">
      <span>Fibre:</span><span>${totals.fibre.toFixed(0)}</span>
    </div>
    <div style="background: #1976d2; color: white; padding: 8px 16px; border-radius: 20px; width: 160px; display: flex; justify-content: space-between;">
      <span>Carbs:</span><span>${totals.carbs.toFixed(0)}</span>
    </div>
    <div style="background: #1976d2; color: white; padding: 8px 16px; border-radius: 20px; width: 160px; display: flex; justify-content: space-between;">
      <span>Fats:</span><span>${totals.fats.toFixed(0)}</span>
    </div>
  </div>
`;







  // Save to DB and show summary
  await window.saveDishRowsToDB(dishEntries);

  const summaryContainer = document.getElementById("summary-container");
  summaryContainer.style.display = "block";
  await window.loadDishSummaryTable();

  // Scroll to summary smoothly
  summaryContainer.scrollIntoView({ behavior: "smooth" });
};



// ✅ Save dish entries to DB
window.saveDishRowsToDB = async function (dishEntries) {
  const today = new Date().toISOString().split("T")[0];
  const userId = localStorage.getItem("user_id");

  if (!userId) {
    console.error("❌ No user_id found in localStorage");
    return;
  }

  // Delete existing dishes for this user and date
  await supabaseClient
    .from("daily_dishes")
    .delete()
    .eq("date", today)
    .eq("user_id", userId);

  const rowsToInsert = [];

  for (const entry of dishEntries) {
    const info = await window.getDishInfo(entry.dish_name);
    if (!info) continue;

    const factor = entry.quantity_grams / 100;
    rowsToInsert.push({
      user_id: userId,
      date: today,
      meal_type: entry.meal_type,
      dish_name: entry.dish_name,
      grams: entry.quantity_grams,
      calories: (info.calorie_per_100gm || 0) * factor,
      protein: (info.protein_per_100gm || 0) * factor,
      carbs: (info.carbs_per_100gm || 0) * factor,
      fibre: (info.fibre_per_100gm || 0) * factor,
      fats: (info.fats_per_100gm || 0) * factor
    });
  }

  if (rowsToInsert.length) {
    await supabaseClient.from("daily_dishes").insert(rowsToInsert);
  }
};


// Load summary table
window.loadDishSummaryTable = async function () {
  const today = new Date().toISOString().split("T")[0];
  const userId = localStorage.getItem("user_id");

  if (!userId) {
    console.error("❌ No user_id found in localStorage");
    return;
  }

  const { data: dishes, error } = await supabaseClient
    .from("daily_dishes")
    .select("*")
    .eq("date", today)
    .eq("user_id", userId);

  if (error) {
    console.error("❌ Error fetching dish summary:", error.message);
    return;
  }

  const tbody = document.getElementById("dish-summary-body");
  tbody.innerHTML = "";

  let totalCalories = 0, totalGrams=0,totalProtein = 0, totalCarbs = 0, totalFibre = 0, totalFats = 0;
  dishes.forEach(dish => {
    const row = document.createElement("tr");
row.innerHTML = `
  <td>${dish.dish_name}</td>
  <td>${dish.grams.toFixed(1)}</td>
  <td>${dish.calories.toFixed(1)}</td>
  <td>${dish.protein.toFixed(1)}</td>
  <td>${dish.fibre.toFixed(1)}</td>
  <td>${dish.carbs.toFixed(1)}</td>
  <td>${dish.fats.toFixed(1)}</td>
`;

    tbody.appendChild(row);
	totalGrams += dish.grams || 0;
    totalCalories += dish.calories || 0;
    totalProtein += dish.protein || 0;
    totalCarbs += dish.carbs || 0;
    totalFibre += dish.fibre || 0;
    totalFats += dish.fats || 0;
  });

  const totalRow = document.createElement("tr");
  totalRow.style.backgroundColor = "#f0f0f0";
  totalRow.style.fontWeight = "bold";
totalRow.innerHTML = `
  <td><strong>Total</strong></td>
  <td><strong>${totalGrams.toFixed(1)}</strong></td>
  <td><strong>${totalCalories.toFixed(1)}</strong></td>
  <td><strong>${totalProtein.toFixed(1)}</strong></td>
  <td><strong>${totalFibre.toFixed(1)}</strong></td>
  <td><strong>${totalCarbs.toFixed(1)}</strong></td>
  <td><strong>${totalFats.toFixed(1)}</strong></td>
`;


  tbody.appendChild(totalRow);
};


window.promptCalorieLogin = async function () {
  const userId = localStorage.getItem("user_id");

  if (userId && userId.trim() !== "") {
    // Verify user in Supabase
    const { data, error } = await supabaseClient
      .from('app_users')
      .select('username')
      .eq('username', userId)
      .maybeSingle();

    if (!error && data) {
      console.log("✅ Valid user found:", userId);
      
      const welcomeDiv = document.getElementById("welcome-message");
      if (welcomeDiv) {
        welcomeDiv.textContent = `Welcome, ${userId}!`;
        welcomeDiv.style.display = "block";
      }

      // ✅ Show the Daily Calorie section
      window.showSection("utility-daily-calorie");
      return;
    } else {
      console.warn("⚠️ Stale user ID found. Clearing.");
      localStorage.removeItem("user_id");
    }
  }

  // Show login modal
  console.log("🔐 No valid login — showing login modal");
  document.getElementById('loginModal').style.display = 'block';
};


// 🔁 Run on app load
window.addEventListener('DOMContentLoaded', () => {
  window.promptCalorieLogin();
});




window.handleCalorieLogin = async function () {
  const username = document.getElementById("usernameInput").value.trim().toLowerCase();
  const password = document.getElementById("passwordInput").value.trim();

  const { data, error } = await supabaseClient
    .from('app_users')
    .select('*')
    .ilike('username', username)  // case-insensitive username
    .eq('password', password)     // case-sensitive password
    .single();

  if (error || !data) {
    alert("Invalid username or password.");
    return;
  }

  // ✅ Only runs if data is present
  localStorage.setItem("user_id", data.username);
  document.getElementById("loginModal").style.display = "none";
  window.showSection('utility-daily-calorie');
 // window.loadDishSummaryTable();
  await window.loadDailyDishes();
};



// Load food facts
window.loadFoodFacts = async function () {
  const { data, error } = await supabaseClient.from("food_items").select("*");
  const tbody = document.querySelector("#food-facts-table tbody");
  tbody.innerHTML = "";

  if (!error && data) {
    data.forEach(dish => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${dish.dish_name}</td>
        <td>${dish.calorie_per_100gm || 0}</td>
        <td>${dish.protein_per_100gm || 0}</td>
        <td>${dish.carbs_per_100gm || 0}</td>
        <td>${dish.fibre_per_100gm || 0}</td>
        <td>${dish.fats_per_100gm || 0}</td>`;
      tbody.appendChild(row);
    });
    enableTableSorting();  // <-- important call added here
  }
};



document.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("nutrition-form");
  if (form) {
    form.addEventListener("submit", async (e) => {
      e.preventDefault();

      const dishName = document.getElementById("dish_name").value.trim();
      const calorie = parseFloat(document.getElementById("calorie").value);
      const protein = parseFloat(document.getElementById("protein").value);
      const carbs = parseFloat(document.getElementById("carbs").value);
      const fibre = parseFloat(document.getElementById("fibre").value);
      const fats = parseFloat(document.getElementById("fats").value);

      const { error } = await supabaseClient.from("food_items").insert([
        {
          dish_name: dishName,
          calorie_per_100gm: calorie,
          protein_per_100gm: protein,
          carbs_per_100gm: carbs,
          fibre_per_100gm: fibre,
          fats_per_100gm: fats
        }
      ]);

      const message = document.getElementById("nutrition-message");
      if (error) {
        message.textContent = "❌ Failed to save dish.";
        message.style.color = "red";
      } else {
        message.textContent = "✅ Dish saved successfully.";
        message.style.color = "green";
        form.reset();
      }
    });
  }
});

function enableTableSorting() {
  const table = document.getElementById("food-facts-table");
  const headers = table.querySelectorAll("th");
  const tbody = table.querySelector("tbody");

  headers.forEach((header, index) => {
    header.style.cursor = "pointer";
    header.addEventListener("click", () => {
      const isAsc = header.classList.contains("asc");
      const rows = Array.from(tbody.querySelectorAll("tr"));
      rows.sort((a, b) => {
        const aVal = parseFloat(a.children[index].textContent) || a.children[index].textContent.toLowerCase();
        const bVal = parseFloat(b.children[index].textContent) || b.children[index].textContent.toLowerCase();
        return (isAsc ? (bVal > aVal ? 1 : -1) : (aVal > bVal ? 1 : -1));
      });
      headers.forEach(h => h.classList.remove("asc", "desc"));
      header.classList.add(isAsc ? "desc" : "asc");
      rows.forEach(row => tbody.appendChild(row));
    });
  });
}

document.addEventListener("DOMContentLoaded", () => {
  const btn = document.getElementById("calculate-btn");
  if (btn) {
    btn.addEventListener("click", window.calculateCalories);
  }
});
window.loadDailyDishes = async function () {
  const today = new Date().toISOString().split("T")[0];
  const userId = localStorage.getItem("user_id");

  if (!userId) {
    console.error("❌ No user_id found in localStorage");
    return;
  }

  const { data, error } = await supabaseClient
    .from("daily_dishes")
    .select("*")
    .eq("date", today)
    .eq("user_id", userId);

  if (error) {
    console.error("Error loading daily dishes:", error);
    return;
  }

  const meals = ["breakfast", "lunch", "dinner"];
  meals.forEach(meal => {
    const container = document.getElementById(`${meal}-container`);
    container.innerHTML = ""; // Clear old rows

    data
      .filter(d => d.meal_type === meal)
      .forEach(dish => {
        window.addDishRow(meal, dish.dish_name, dish.grams);
      });
  });
};

let deferredPrompt;
const installBtn = document.getElementById('installApp');

// Listen for the install prompt event
window.addEventListener('beforeinstallprompt', (e) => {
  e.preventDefault(); // Prevent auto-prompt
  deferredPrompt = e;
  installBtn.style.display = 'inline-block'; // Show the button
});

// Handle click on install button
installBtn.addEventListener('click', () => {
  if (deferredPrompt) {
    deferredPrompt.prompt();
    deferredPrompt.userChoice.then(choiceResult => {
      console.log('User response to the install prompt:', choiceResult.outcome);
      deferredPrompt = null;
      installBtn.style.display = 'none';
    });
  }
});

if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('service-worker.js').then(registration => {
    console.log('✅ SW registered');

    registration.onupdatefound = () => {
      const newSW = registration.installing;
      newSW.onstatechange = () => {
        if (newSW.state === 'installed') {
          if (navigator.serviceWorker.controller) {
            console.log('🔄 New version available — reloading...');
            window.location.reload(); // Reload to activate the new SW
          }
        }
      };
    };
  });
}

if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('./service-worker.js').then(registration => {
    registration.onupdatefound = () => {
      const newWorker = registration.installing;
      newWorker.onstatechange = () => {
        if (newWorker.state === 'installed' && navigator.serviceWorker.controller) {
          console.log("New version available. Reloading...");
          window.location.reload(); // 🔁 Force reload to apply new cache
        }
      };
    };
  });
}

// Weight Tracker Script
window.weightEntries = [];

window.handleWeightUpload = function (event) {
  event.preventDefault();
  const date = document.getElementById("weight-date").value;
  const weight = document.getElementById("weight-value").value;
  const fileInput = document.getElementById("weight-image");

  if (fileInput.files.length > 0) {
    const reader = new FileReader();
    reader.onload = function (e) {
      const imageSrc = e.target.result;
      window.weightEntries.push({ date, weight, imageSrc });
      window.updateWeightTimeline();
    };
    reader.readAsDataURL(fileInput.files[0]);
  }
};

window.updateWeightTimeline = function () {
  const list = document.getElementById("timeline-list");
  list.innerHTML = "";
  window.weightEntries.slice().reverse().forEach(entry => {
    const item = document.createElement("li");
    item.style.marginBottom = "15px";
    item.innerHTML = `<strong>${entry.date}</strong> - ${entry.weight} kg<br><img src="${entry.imageSrc}" style="width:100px; height:auto; border:1px solid #ccc; margin-top:5px;">`;
    list.appendChild(item);
  });
};

// Attach event listener after DOM is ready
window.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("weight-upload-form");
  if (form) {
    form.addEventListener("submit", window.handleWeightUpload);
  }
});



window.toggleSidebar = function () {
  const sidebar = document.getElementById("sidebar");
  if (sidebar) {
    sidebar.classList.toggle("open");
    console.log("✅ Sidebar toggled:", sidebar.className);
  } else {
    console.error("❌ Sidebar not found");
  }
};

document.addEventListener("DOMContentLoaded", () => {
  const userId = localStorage.getItem("user_id");
  const welcomeDiv = document.getElementById("welcome-message");

  if (userId && welcomeDiv) {
    welcomeDiv.textContent = `Welcome, ${userId}!`;
    welcomeDiv.style.display = "block";
  }
});



document.addEventListener("click", function (event) {
  setTimeout(() => {
    const sidebar = document.getElementById("sidebar");
    const hamburger = document.querySelector(".hamburger");  // ✅ FIXED HERE

    if (!sidebar || !hamburger) {
      console.log("🔍 Sidebar or hamburger not found");
      return;
    }

    const isSidebarOpen = sidebar.classList.contains("open");
    const clickedInsideSidebar = sidebar.contains(event.target);
    const clickedInsideHamburger = hamburger.contains(event.target);

    console.log("🖱️ Clicked element:", event.target);
    console.log("📍 Inside sidebar:", clickedInsideSidebar);
    console.log("📍 On hamburger:", clickedInsideHamburger);
    console.log("📂 Sidebar open state:", isSidebarOpen);

    if (isSidebarOpen && !clickedInsideSidebar && !clickedInsideHamburger) {
      sidebar.classList.remove("open");
      console.log("✅ Sidebar closed.");
    }
  }, 10); // slight delay to ensure toggle has completed
});

window.addBikeRecordSection = async function () {
  console.log("🚴 Loading Add Bike Record section");

  const container = document.getElementById("bike-section-container");
  if (!container) {
    console.error("❌ Container 'bike-section-container' not found.");
    return;
  }

  // Hide default home section
  const defaultHome = document.getElementById("utility-daily-calorie");
  if (defaultHome) defaultHome.style.display = "none";

  try {
    const response = await fetch("bike-history.html");
    const html = await response.text();
    container.innerHTML = html;
    console.log("✅ Add Bike Record HTML loaded.");

    if (typeof initAddBikeRecordSection === "function") {
      initAddBikeRecordSection();
    }

  } catch (err) {
    console.error("❌ Failed to load bike-history.html:", err);
  }
};

window.initAddBikeRecordSection = function () {
  console.log("🚀 Initializing Add Bike Record logic");

  const submitBtn = document.getElementById("submitBikeRecordBtn");
  if (!submitBtn) {
    console.warn("⛔ Submit button not found.");
    return;
  }

  submitBtn.addEventListener("click", async () => {
    const date = document.getElementById("date_changed").value;
    const amount = parseFloat(document.getElementById("amount").value);
    const distance = parseFloat(document.getElementById("at_distance").value);
    const user_id = localStorage.getItem("user_id");

    if (!date || isNaN(amount) || isNaN(distance) || !user_id) {
      alert("Please fill all fields and ensure user is logged in.");
      return;
    }

    try {
      const { data, error } = await supabaseClient.from("bike_history").insert([
        { user_id, date_changed: date, amount, at_distance: distance }
      ]);
      if (error) throw error;

      document.getElementById("record_status").textContent = "✅ Record added!";
    } catch (err) {
      document.getElementById("record_status").textContent = "❌ Failed to add record.";
      console.error(err);
    }
  });
};


window.loadBikeHistorySection = async function () {
  const historyContainer = document.getElementById("bike-history-container");
  const formContainer = document.getElementById("bike-section-container");
  const homeSection = document.getElementById("utility-daily-calorie");

  if (formContainer) formContainer.style.display = "none";
  if (homeSection) homeSection.style.display = "none";

  if (!historyContainer) {
    console.error("❌ #bike-history-container not found");
    return;
  }

  historyContainer.style.display = "block";

  const user_id = localStorage.getItem("user_id");
  if (!user_id) {
    historyContainer.innerHTML = "<p>⚠️ Not logged in.</p>";
    return;
  }

  try {
    const { data, error } = await supabaseClient
      .from("bike_history")
      .select("*")
      .eq("user_id", user_id)
      .order("date_changed", { ascending: false });

    if (error) throw error;

    if (!data || data.length === 0) {
      historyContainer.innerHTML = "<p>No records found.</p>";
      return;
    }

	let htmlTable = `
  <style>
    .bike-table-container {
      margin-top: 40px;
      margin-left: 40px;
    }
    .bike-table {
      border-collapse: collapse;
      font-size: 14px;
      table-layout: fixed;
      border: 1px solid #ccc;
      box-shadow: 0 0 8px rgba(0, 0, 0, 0.05);
      font-family: "Segoe UI", sans-serif;
    }
    .bike-table th {
      background-color: #003366;
      color: white;
      padding: 8px 12px;
      border: 1px solid #ccc;
      text-align: center;
      font-weight: bold;
    }
    .bike-table td {
      border: 1px solid #ccc;
      padding: 4px 6px;
      text-align: center;
    }
    .bike-table tr:nth-child(even) td {
      background-color: #f2f8fc;
    }
    .bike-table tr:nth-child(odd) td {
      background-color: #ffffff;
    }
    .bike-table tr:hover td {
      background-color: #d6ebff;
    }
  </style>

  <div class="bike-table-container">
    <table class="bike-table">
      <thead>
        <tr>
          <th style="white-space: nowrap;">Date</th>
          <th>Odometer</th>
          <th>Amount</th>
        </tr>
      </thead>
      <tbody>
`;

data.forEach((row) => {
  const formattedDate = new Date(row.date_changed).toLocaleDateString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric"
  }).toUpperCase().replace(/ /g, "-");

  htmlTable += `
    <tr>
    <td>${formattedDate}</td>
    <td>${row.at_distance} km</td>
    <td>₹${row.amount}</td>
    </tr>
  `;
});

htmlTable += `
      </tbody>
    </table>
  </div>
`;



    historyContainer.innerHTML = htmlTable;

  } catch (err) {
    console.error("❌ Failed to load bike history:", err);
    historyContainer.innerHTML = "<p>❌ Error loading history.</p>";
  }
};


window.loadBikeSummary = async function () {
  const container = document.getElementById("bike-summary-container");

  // Hide the home page section
  const home = document.getElementById("utility-daily-calorie");
  if (home) home.style.display = "none";

  // Hide all bike sections
  document.querySelectorAll(".bike-section").forEach(sec => {
    sec.style.display = "none";
  });

  // Show the summary container only
  container.style.display = "block";
  container.innerHTML = "<b>Loading summary...</b>";

  // Get logged-in user id from localStorage
  const userId = localStorage.getItem("user_id");
  if (!userId) {
    container.innerHTML = `<span style="color:orange;">No user logged in. Please log in to view bike summary.</span>`;
    return;
  }

  try {
    const { data: records, error } = await supabaseClient
      .from('bike_history')
      .select('*')
      .eq('user_id', userId)
      .order('date_changed', { ascending: true });

    if (error) {
      container.innerHTML = `<span style="color:red;">Error loading data: ${error.message}</span>`;
      return;
    }

    if (!records || records.length < 2) {
      container.innerHTML = `<span style="color: orange;">Not enough bike records to calculate summary.</span>`;
      return;
    }

    // === Expense calculation: Exclude latest entry ===
    let totalCost = 0;
    for (let i = 0; i < records.length - 1; i++) {
      totalCost += records[i].amount;
    }

    let monthly = {};
    let weekly = {};
    let firstDistance = records[0].at_distance;
    let lastDistance = records[records.length - 1].at_distance;
    let totalKm = lastDistance - firstDistance;

    // For breakdowns, you may also want to exclude the last entry if needed.
    // (Optional: If you want monthly/weekly breakdowns to also exclude the last entry, adjust logic here.)
    records.forEach((rec, idx) => {
      // Exclude the last entry from monthly/weekly cost and km if you want
      let include = idx < records.length - 1;

      // --- Monthly ---
      const month = rec.date_changed.slice(0, 7); // 'YYYY-MM'
      if (!monthly[month]) monthly[month] = { cost: 0, first: rec.at_distance, last: rec.at_distance };
      if (include) monthly[month].cost += rec.amount;
      if (rec.at_distance < monthly[month].first) monthly[month].first = rec.at_distance;
      if (rec.at_distance > monthly[month].last) monthly[month].last = rec.at_distance;

      // --- Weekly ---
      const d = new Date(rec.date_changed);
      const year = d.getFullYear();
      const week = getWeekNumber(d);
      const weekKey = `${year}-W${week}`;
      if (!weekly[weekKey]) weekly[weekKey] = { cost: 0, first: rec.at_distance, last: rec.at_distance };
      if (include) weekly[weekKey].cost += rec.amount;
      if (rec.at_distance < weekly[weekKey].first) weekly[weekKey].first = rec.at_distance;
      if (rec.at_distance > weekly[weekKey].last) weekly[weekKey].last = rec.at_distance;
    });

    for (let key in monthly) monthly[key].km = monthly[key].last - monthly[key].first;
    for (let key in weekly) weekly[key].km = weekly[key].last - weekly[key].first;

    let mileage = totalCost > 0 ? (totalKm / (totalCost / 103)).toFixed(2) : "N/A";

    let html = `
<style>
  .bike-summary-details {
    margin-bottom: 10px;
    border-radius: 8px;
    background: #fcfcfc;
    box-shadow: 0 2px 8px #f6f6f6;
    border: 1px solid #eee;
    padding: 0;
    max-width: 600px;
  }
  .bike-summary-details summary {
    font-size: 1.15em;
    font-weight: bold;
    color: #222;
    cursor: pointer;
    padding: 12px 20px;
    outline: none;
    border-radius: 8px 8px 0 0;
    display: flex;
    align-items: center;
    gap: 12px;
    background: #f4f8ff;
    border-bottom: 1px solid #ececec;
    margin: 0;
    transition: background 0.2s;
  }
  .bike-summary-details[open] summary {
    background: #e5edfb;
  }
  .bike-summary-details summary .summary-title {
    flex: 1;
    text-align: left;
    font-size: 1.08em;
  }
  .bike-summary-summary    { color: #2d5be3; }
  .bike-summary-monthly    { color: #f7b500; }
  .bike-summary-weekly     { color: #fd7c25; }
  .bike-summary-details > *:not(summary) {
    padding: 10px 26px 15px 26px;
    margin: 0;
  }
  .bike-summary-ul {
    list-style: none;
    padding-left: 0;
    margin: 8px 0 0 0;
  }
  .bike-summary-ul li {
    margin: 8px 0;
    font-size: 1.05em;
  }
  .bike-summary-table {
    width: 100%;
    margin: 10px 0;
    border-collapse: collapse;
    background: #fff7ec;
    box-shadow: 0 2px 8px #f0e9e3;
    border-radius: 8px;
    overflow: hidden;
  }
  .bike-summary-table th {
    background: #f7b500;
    color: #222;
    padding: 7px 12px;
    font-weight: bold;
  }
  .bike-summary-table td {
    padding: 7px 12px;
    text-align: center;
    border-bottom: 1px solid #ffe7b6;
  }
  .bike-summary-table tr:last-child td {
    border-bottom: none;
  }
  .bike-summary-highlight {
    font-weight: bold;
    color: #17b317;
    font-size: 1.08em;
  }
</style>

<details class="bike-summary-details" open>
  <summary>
    <span>🚦</span>
    <span class="summary-title bike-summary-summary">Summary</span>
  </summary>
  <ul class="bike-summary-ul">
    <li>⛽ <b>Total Cost:</b> <span class="bike-summary-highlight">₹${totalCost}</span></li>
    <li>🏍️ <b>Total KM Driven:</b> <span class="bike-summary-highlight">${totalKm} km</span></li>
    <li>🛣️ <b>Bike Fuel Mileage:</b> <span class="bike-summary-highlight">${mileage} km/l</span></li>
  </ul>
</details>
<details class="bike-summary-details">
  <summary>
    <span>📅</span>
    <span class="summary-title bike-summary-monthly">Monthly Summary</span>
  </summary>
  <table class="bike-summary-table">
    <tr><th>Month</th><th>Cost (₹)</th><th>KM</th></tr>
    ${Object.entries(monthly).map(([m, v]) =>
      `<tr><td>${m}</td><td>${v.cost}</td><td>${v.km}</td></tr>`
    ).join('')}
  </table>
</details>
<details class="bike-summary-details">
  <summary>
    <span>📆</span>
    <span class="summary-title bike-summary-weekly">Weekly Summary</span>
  </summary>
  <table class="bike-summary-table">
    <tr><th>Week</th><th>Cost (₹)</th><th>KM</th></tr>
    ${Object.entries(weekly).map(([w, v]) =>
      `<tr><td>${w}</td><td>${v.cost}</td><td>${v.km}</td></tr>`
    ).join('')}
  </table>
</details>
`;

    container.innerHTML = html;

  } catch (err) {
    container.innerHTML = `<span style="color:red;">❌ Error loading bike summary: ${err.message}</span>`;
  }
}

// Helper: ISO week number
function getWeekNumber(d) {
  d = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}
